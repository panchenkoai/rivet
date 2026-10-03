//! Idempotent bucket provisioning for the object-storage destinations
//! (MinIO for S3, fake-gcs for GCS).

#![allow(dead_code)]

use std::net::TcpStream;
use std::process::Command;

use super::env::{
    AZURITE_CONN_STRING, LiveService, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY,
    require_alive,
};

/// Idempotently create `bucket` in the local MinIO; an existing bucket is success.
pub fn ensure_minio_bucket(bucket: &str) {
    require_alive(LiveService::Minio);
    let out = minio_mc(&format!("mc mb --ignore-existing local/{bucket}"))
        .output()
        .expect("spawn `docker exec` — live S3/MinIO tests need the docker CLI on PATH");
    assert!(
        out.status.success(),
        "`mc mb --ignore-existing local/{bucket}` in container `{}` failed ({}):\n{}{}",
        minio_container(),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The running container publishing MinIO's host port, found by port so no compose project name is needed.
pub fn minio_container() -> &'static str {
    static NAME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    NAME.get_or_init(|| {
        let port = MINIO_ENDPOINT
            .rsplit(':')
            .next()
            .expect("MINIO_ENDPOINT has a port");
        container_for_port(port)
    })
}

/// The name of the running container that publishes host `port`; panics naming the port when none does.
fn container_for_port(port: &str) -> String {
    let out = Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("publish={port}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .expect("spawn `docker ps` — live S3/MinIO tests need the docker CLI on PATH");
    assert!(
        out.status.success(),
        "`docker ps --filter publish={port}` failed ({}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .map(str::to_string)
        .unwrap_or_else(|| {
            panic!(
                "no running container publishes host port {port} (MinIO, {MINIO_ENDPOINT}) — \
                 start the stand's `minio` service"
            )
        })
}

/// `docker exec -i <minio> sh -c "mc alias set local … && <mc>"`: an `mc` command against the local MinIO.
pub fn minio_mc(mc: &str) -> Command {
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null && {mc}"
    );
    let mut cmd = Command::new("docker");
    cmd.args(["exec", "-i", minio_container(), "sh", "-c", &script]);
    cmd
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
    let ls = minio_mc(&format!("mc ls --recursive local/{bucket} 2>/dev/null"))
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
        let cat = minio_mc(&format!("mc cat local/{bucket}/{name}"))
            .output()
            .expect("mc cat");
        assert!(cat.status.success(), "mc cat {name} failed");
        total += super::parquet_rows_from_bytes(cat.stdout);
    }
    total
}

/// Same independent oracle for fake-gcs: list the objects via the JSON API,
/// then `GET ?alt=media` each `.parquet` and sum the row counts.
/// Object NAMES under a fake-gcs prefix, from the store's own JSON API.
///
/// Extracted because DuckDB cannot list this emulator — its JSON API answers a
/// GET but 404s the HEAD `httpfs` issues to size a file (measured) — so the
/// store census takes the names from here and reads the objects itself. One
/// lister, used by both, rather than a second HTTP block that drifts.
pub fn fake_gcs_object_names(bucket: &str, prefix: &str) -> Vec<String> {
    let list = fake_gcs_list_json(bucket, prefix);
    let items = list["items"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    assert!(
        !items.is_empty(),
        "fake_gcs_object_names: prefix `{prefix}` in bucket `{bucket}` matched \
         no objects — a wrong prefix reads exactly like an empty export"
    );
    items
        .iter()
        .filter_map(|i| i["name"].as_str().map(|s| s.to_string()))
        .collect()
}

fn fake_gcs_list_json(bucket: &str, prefix: &str) -> serde_json::Value {
    use std::io::{Read, Write};
    let http = |req: String| -> Vec<u8> {
        let mut s = TcpStream::connect("127.0.0.1:4443").expect("connect fake-gcs");
        s.write_all(req.as_bytes()).expect("write fake-gcs req");
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf);
        let sep = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("http header separator");
        buf.split_off(sep + 4)
    };
    let list = http(format!(
        "GET /storage/v1/b/{bucket}/o?prefix={prefix} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    ));
    serde_json::from_slice(&list).expect("fake-gcs list JSON")
}

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

/// Pull every object under `prefix` into a LOCAL directory, each at its key relative to
/// `prefix` (sub-prefixes become sub-directories), and return how many were written. A
/// missing bucket pulls nothing.
///
/// This exists so a cloud destination can be graded by the SAME oracle as a local one:
/// a store-specific "what was delivered" reader is a second definition of delivered,
/// and it drifted on the first fix (resume cells on s3/gcs read 2000 rows from a
/// 1000-row table). Keys keep their sub-prefixes: a CDC destination nests `snapshot/`
/// and per-table prefixes whose manifest names collide when flattened.
pub fn minio_pull_prefix(bucket: &str, prefix: &str, into: &std::path::Path) -> usize {
    let ls = minio_mc(&format!("mc ls --recursive --json local/{bucket}"))
        .output()
        .expect("mc ls");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&ls.stdout),
        String::from_utf8_lossy(&ls.stderr)
    );
    if !ls.status.success()
        && (said.contains("does not exist") || said.contains("bucket is not valid"))
    {
        return 0;
    }
    assert!(ls.status.success(), "mc ls local/{bucket} failed: {said}");
    // JSON keys, never whitespace-split text: a key may hold a space.
    let names: Vec<String> = String::from_utf8_lossy(&ls.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["key"].as_str().map(String::from))
        .filter(|n| n.starts_with(prefix))
        .collect();
    write_pulled(prefix, into, names, false, |name| {
        let cat = minio_mc(&format!(
            "mc cat 'local/{bucket}/{}'",
            name.replace('\'', "'\\''")
        ))
        .output()
        .expect("mc cat");
        assert!(cat.status.success(), "mc cat {name} failed");
        cat.stdout
    })
}

/// Write each object `name` (fetched by `get`) under `into` at its key relative to `prefix`, percent-decoded when `decode`.
fn write_pulled(
    prefix: &str,
    into: &std::path::Path,
    names: Vec<String>,
    decode: bool,
    get: impl Fn(&str) -> Vec<u8>,
) -> usize {
    std::fs::create_dir_all(into).expect("create pull dir");
    let mut seen = std::collections::BTreeSet::new();
    for name in &names {
        let rel = name[prefix.len()..].trim_start_matches('/');
        let rel = if rel.is_empty() {
            name.rsplit('/').next().unwrap_or(name)
        } else {
            rel
        };
        let rel = if decode {
            percent_encoding::percent_decode_str(rel)
                .decode_utf8_lossy()
                .to_string()
        } else {
            rel.to_string()
        };
        let rel = rel.as_str();
        assert!(
            seen.insert(rel.to_string()),
            "two objects under {prefix} map to {rel}"
        );
        let path = into.join(rel);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create pull sub-dir");
        std::fs::write(&path, get(name)).expect("write pulled object");
    }
    names.len()
}

/// Pull every object under `prefix` in a GCS bucket through the JSON API at `base` (fake-gcs, or
/// `https://storage.googleapis.com` with a bearer `token`), like [`minio_pull_prefix`]; a missing bucket pulls nothing.
pub fn gcs_pull_prefix(
    base: &str,
    bucket: &str,
    prefix: &str,
    token: Option<&str>,
    into: &std::path::Path,
) -> usize {
    let http = reqwest::blocking::Client::new();
    // A transport error (not a status) is retried twice: the real endpoint drops a connection now and then.
    let get = |url: &str, query: &[(&str, &str)]| {
        let mut last = None;
        for _ in 0..3 {
            let mut req = http.get(url).query(query);
            if let Some(t) = token {
                req = req.bearer_auth(t);
            }
            match req.send() {
                Ok(r) => return r,
                Err(e) => last = Some(e),
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!("GCS GET {url}: {}", last.expect("an error"))
    };
    let list_url = format!("{base}/storage/v1/b/{bucket}/o");
    let (mut names, mut page) = (Vec::new(), String::new());
    loop {
        let resp = get(&list_url, &[("prefix", prefix), ("pageToken", &page)]);
        if resp.status() == 404 {
            return 0;
        }
        assert!(
            resp.status().is_success(),
            "GCS list {bucket}/{prefix}: {}",
            resp.status()
        );
        let doc: serde_json::Value = resp.json().expect("GCS list JSON");
        names.extend(
            doc["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|i| i["name"].as_str().map(String::from)),
        );
        match doc["nextPageToken"].as_str() {
            Some(t) => page = t.to_string(),
            None => break,
        }
    }
    // ponytail: fake-gcs stores the keys rivet writes through opendal with `=` as a literal `%3D` (real GCS
    // stores `=`, measured 2026-10-01), so an emulator pull decodes the key; a real-GCS pull never does.
    write_pulled(prefix, into, names, token.is_none(), |name| {
        let url = format!("{list_url}/{}", urlencoding_path(name));
        let resp = get(&url, &[("alt", "media")]);
        assert!(
            resp.status().is_success(),
            "GCS GET {name}: {}",
            resp.status()
        );
        resp.bytes().expect("GCS object body").to_vec()
    })
}

/// An object name as one percent-encoded URL path segment.
fn urlencoding_path(name: &str) -> String {
    name.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Pull every blob under `prefix` in an azurite container (anonymous `List Blobs` and GET: the
/// container is public-read, see [`ensure_azure_container`]), like [`minio_pull_prefix`]; a
/// missing container pulls nothing.
pub fn azure_pull_prefix(
    endpoint: &str,
    container: &str,
    prefix: &str,
    into: &std::path::Path,
) -> usize {
    let http = reqwest::blocking::Client::new();
    let (mut names, mut marker) = (Vec::new(), String::new());
    loop {
        let resp = http
            .get(format!("{endpoint}/{container}"))
            .query(&[
                ("restype", "container"),
                ("comp", "list"),
                ("prefix", prefix),
                ("marker", &marker),
            ])
            .send()
            .expect("azure list request");
        if resp.status() == 404 {
            return 0;
        }
        assert!(
            resp.status().is_success(),
            "azure list {container}/{prefix}: {}",
            resp.status()
        );
        let xml = resp.text().expect("azure list body");
        names.extend(azure_blob_names_from_list_xml(&xml));
        match xml
            .split("<NextMarker>")
            .nth(1)
            .and_then(|s| s.split("</NextMarker>").next())
        {
            Some(m) if !m.is_empty() => marker = m.to_string(),
            _ => break,
        }
    }
    write_pulled(prefix, into, names, false, |name| {
        let resp = http
            .get(format!("{endpoint}/{container}/{name}"))
            .send()
            .expect("azure blob download");
        assert!(
            resp.status().is_success(),
            "azure GET {name}: {}",
            resp.status()
        );
        resp.bytes().expect("azure blob body").to_vec()
    })
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

/// Write `bytes` to `key` in a fake-gcs bucket through the JSON API's media upload.
pub fn fake_gcs_put(bucket: &str, key: &str, bytes: &[u8]) {
    let resp = reqwest::blocking::Client::new()
        .post(format!(
            "http://127.0.0.1:4443/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
            key.replace('/', "%2F")
        ))
        .body(bytes.to_vec())
        .send()
        .expect("fake-gcs upload");
    assert!(
        resp.status().is_success(),
        "fake-gcs upload of {key}: {}",
        resp.status()
    );
}

/// Object names under `prefix` in a fake-gcs bucket; empty when none match.
pub fn fake_gcs_names(bucket: &str, prefix: &str) -> Vec<String> {
    fake_gcs_list_json(bucket, prefix)["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|i| i["name"].as_str().map(String::from))
        .collect()
}

/// Write `bytes` to `key` in a MinIO bucket through `mc pipe` inside the container.
pub fn minio_put(bucket: &str, key: &str, bytes: &[u8]) {
    use std::io::Write;
    let mut child = minio_mc(&format!("mc pipe local/{bucket}/{key}"))
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn mc pipe");
    child
        .stdin
        .take()
        .expect("mc pipe stdin")
        .write_all(bytes)
        .expect("write mc pipe");
    assert!(
        child.wait().expect("mc pipe").success(),
        "mc pipe {key} failed"
    );
}

/// Object names under `prefix` in a MinIO bucket; empty when none match.
pub fn minio_object_names(bucket: &str, prefix: &str) -> Vec<String> {
    let ls = minio_mc(&format!("mc ls --recursive local/{bucket}"))
        .output()
        .expect("mc ls");
    assert!(ls.status.success(), "mc ls local/{bucket} failed");
    String::from_utf8_lossy(&ls.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().last())
        .filter(|n| n.starts_with(prefix))
        .map(String::from)
        .collect()
}

/// Write `bytes` to blob `key` in an azurite container with the `az` CLI.
pub fn azure_put(container: &str, key: &str, bytes: &[u8]) {
    let file = tempfile::NamedTempFile::new().expect("blob temp file");
    std::fs::write(file.path(), bytes).expect("write blob temp file");
    let out = Command::new("az")
        .args([
            "storage",
            "blob",
            "upload",
            "--container-name",
            container,
            "--name",
            key,
            "--file",
            file.path().to_str().expect("utf-8 temp path"),
            "--overwrite",
            "--connection-string",
            AZURITE_CONN_STRING,
        ])
        .output()
        .expect("spawn az storage blob upload");
    assert!(
        out.status.success(),
        "az storage blob upload {key}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
