use opendal::Operator;
use opendal::services::Gcs;
use std::sync::Arc;

use super::cloud::{CloudBackend, CloudDestination};
use super::gcs_auth;
use crate::config::DestinationConfig;
use crate::error::Result;

/// Build the async GCS [`Operator`] for `config`, using the same auth chain
/// (ADC refreshing loader / credentials_file / anonymous emulator) as the
/// streaming export destination. Reused by the load layer's one-off object ops
/// so they never shell out to `gcloud` and never hand-roll a second auth path.
/// The object-store operator for `config`'s destination: GCS, S3 or Azure.
pub(crate) fn operator_for(config: &DestinationConfig) -> Result<Operator> {
    match config.destination_type {
        crate::config::DestinationType::S3 => super::s3::S3Backend::build_operator(config),
        crate::config::DestinationType::Azure => super::azure::AzureBackend::build_operator(config),
        _ => GcsBackend::build_operator(config),
    }
}

/// Scope a bucket-relative path to a DIRECTORY boundary for prefix listing and
/// recursive delete. opendal (and GCS/S3 under it) match by STRING prefix, so a
/// non-slash `exports/orders` also matches `exports/orders_archive/…`; the
/// trailing slash confines the op to the directory. Empty stays empty (the
/// bucket root is refused upstream by `split_object_uri`, never reached here).
fn dir_boundary(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

/// Append one streamed chunk; `Some(len)` once the body has grown past `cap` (stop reading).
fn push_within(buf: &mut Vec<u8>, chunk: &[u8], cap: u64) -> Option<u64> {
    buf.extend_from_slice(chunk);
    (buf.len() as u64 > cap).then_some(buf.len() as u64)
}

/// A blocking GCS handle for the load layer's one-off object ops — recursive
/// list (manifests / parquet), read (manifest bytes), and recursive delete
/// (source cleanup). Mirrors [`CloudDestination`]'s runtime + blocking wrap,
/// but exposes the read/list/delete surface the streaming `Destination` trait
/// does not. Holds the runtime the blocking operator drives.
pub(crate) struct GcsStore {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: opendal::blocking::Operator,
    /// The same operator unwrapped, for reads issued concurrently.
    async_op: Operator,
}

impl GcsStore {
    /// Build a blocking GCS store for `config`'s bucket. Paths passed to the
    /// methods below are **bucket-relative** (no `gs://bucket/` prefix).
    pub(crate) fn new(config: &DestinationConfig) -> Result<Self> {
        Self::wrap(operator_for(config)?)
    }

    fn wrap(async_op: Operator) -> Result<Self> {
        // ONE runtime for every store: a store is built per load item and `--pool N`
        // drops them while other workers reuse the global HTTP pool's connections.
        let runtime = super::cloud::io_runtime()?;
        let _guard = runtime.enter();
        // Retry transient HTTP failures (5xx / 429 / hyper-reqwest blips) on the
        // LOAD/read path too — the export path (CloudDestination) applies this
        // identical RetryLayer, but this store built the operator without it, so
        // a transient blip during a load list/read failed hard (bug hunt
        // 2026-08-09). One policy on both paths; harmless on the local Fs backend.
        let async_op = async_op.layer(
            opendal::layers::RetryLayer::new()
                .with_max_times(3)
                .with_min_delay(std::time::Duration::from_millis(200))
                .with_max_delay(std::time::Duration::from_secs(10))
                .with_jitter()
                .with_notify(super::cloud::RivetRetryNotify),
        );
        let op = opendal::blocking::Operator::new(async_op.clone())?;
        Ok(Self {
            _runtime: runtime,
            op,
            async_op,
        })
    }

    /// Bucket-root-relative keys of every FILE recursively under `path`.
    pub(crate) fn list_files(&self, path: &str) -> Result<Vec<String>> {
        let dir = dir_boundary(path);
        let listed = self.op.list_options(
            &dir,
            opendal::options::ListOptions {
                recursive: true,
                ..Default::default()
            },
        )?;
        Ok(listed
            .into_iter()
            .filter(|e| e.metadata().mode() == opendal::EntryMode::FILE)
            .map(|e| e.path().to_string())
            .collect())
    }

    /// Byte size of the single object at the bucket-relative `path` — a metadata
    /// `stat` (a recursive `list` does not reliably carry each object's length).
    pub(crate) fn stat_size(&self, path: &str) -> Result<u64> {
        Ok(self.op.stat(path)?.content_length())
    }

    /// Raw bytes of the object at the bucket-relative `path`.
    pub(crate) fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self.op.read(path)?.to_vec())
    }

    /// `parse(path, body)` over each object in `paths`, in order, 16 in flight. `body` is
    /// the object's bytes, or `Err(size)` when it is over `cap`: a stat refuses it before
    /// any byte is read, and the read itself stops past `cap` — so an object rewritten
    /// in place between the two (a running marker becoming its terminal manifest) is
    /// read whole when it still fits, and refused when it grew past the cap. A path
    /// deleted after it was listed is left out, not an error.
    pub(crate) fn read_each_within<T>(
        &self,
        paths: &[String],
        cap: u64,
        parse: impl Fn(&str, std::result::Result<Vec<u8>, u64>) -> Result<T>,
    ) -> Result<Vec<T>> {
        use futures_util::{StreamExt, TryStreamExt};
        let (op, parse) = (&self.async_op, &parse);
        self._runtime.block_on(
            futures_util::stream::iter(paths)
                .map(|p| async move {
                    let Some(meta) = unless_gone(op.stat(p).await)? else {
                        return Ok(None);
                    };
                    let size = meta.content_length();
                    if size > cap {
                        return parse(p, Err(size)).map(Some);
                    }
                    let Some(reader) = unless_gone(op.reader(p).await)? else {
                        return Ok(None);
                    };
                    let mut chunks = reader.into_bytes_stream(..).await?;
                    let mut buf = Vec::with_capacity(size as usize);
                    while let Some(chunk) = chunks.try_next().await? {
                        if let Some(over) = push_within(&mut buf, &chunk, cap) {
                            return parse(p, Err(over)).map(Some);
                        }
                    }
                    parse(p, Ok(buf)).map(Some)
                })
                .buffered(16)
                .try_filter_map(|x| async move { Ok(x) })
                .try_collect(),
        )
    }

    /// `len` bytes of the object at the bucket-relative `path`, from offset `start`.
    pub(crate) fn read_range(&self, path: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        let opts = opendal::options::ReadOptions {
            range: (start..start + len).into(),
            ..Default::default()
        };
        Ok(self.op.read_options(path, opts)?.to_vec())
    }

    /// Recursively delete everything under the bucket-relative `path`.
    ///
    /// Normalise to a DIRECTORY boundary first (`dir_boundary`): opendal — and
    /// GCS/S3 under it — match by STRING prefix, so `remove_all("exports/orders")`
    /// would ALSO delete `exports/orders_archive/…`, `exports/orders2/…`, and any
    /// other object whose key string-starts-with it. `list_files` always scoped
    /// with a trailing slash; this delete path did NOT, so a post-load source
    /// cleanup could destroy UNRELATED sibling exports. The fs backend reproduces
    /// it too (opendal string-prefixes there as well) — the prior "spares
    /// siblings" test only used a non-prefix sibling (`keep/`), so it never
    /// activated the bug.
    pub(crate) fn remove_all(&self, path: &str) -> Result<()> {
        self.op.remove_all(&dir_boundary(path))?;
        Ok(())
    }

    /// Delete the single object at the bucket-relative `path`. Deleting a missing
    /// object is a no-op `Ok` — opendal's delete is idempotent.
    pub(crate) fn remove(&self, path: &str) -> Result<()> {
        self.op.delete(path)?;
        Ok(())
    }

    /// A store backed by a local filesystem root — for offline tests of the
    /// load layer's list/read/delete logic without a live bucket.
    #[cfg(test)]
    pub(crate) fn open_fs(root: &str) -> Result<Self> {
        Self::wrap(Operator::new(opendal::services::Fs::default().root(root))?.finish())
    }

    /// The runtime this store drives its operator on.
    #[cfg(test)]
    pub(crate) fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self._runtime
    }

    /// Write `bytes` to the bucket-relative `path`. The load store is otherwise
    /// read/list/delete-only; this is a test-only seam for STAGING objects into a
    /// live/emulated bucket (the fake-gcs-server contract test seeds manifests +
    /// parts through it, then exercises the real list/read/remove path).
    #[cfg(test)]
    pub(crate) fn put(&self, path: &str, bytes: &[u8]) -> Result<()> {
        self.op.write(path, bytes.to_vec())?;
        Ok(())
    }
}

/// GCS object-store destination. The retry policy, blocking wrap, and ADR-0013
/// read surface live in [`CloudDestination`]; this type only knows how to
/// authenticate against Google Cloud Storage.
pub type GcsDestination = CloudDestination<GcsBackend>;

/// Zero-sized backend marker carrying GCS's operator construction.
pub struct GcsBackend;

impl CloudBackend for GcsBackend {
    const SCHEME: &'static str = "gs";

    fn build_operator(config: &DestinationConfig) -> Result<Operator> {
        let bucket = config
            .bucket
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GCS destination requires 'bucket'"))?;

        let mut builder = Gcs::default().bucket(bucket);

        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint(endpoint);
        }

        if config.allow_anonymous {
            builder = builder
                .allow_anonymous()
                .disable_vm_metadata()
                .disable_config_load();
            log::info!("GCS: allow_anonymous (emulator mode; no OAuth / service account)");
        } else if let Some(cred_file) = &config.credentials_file {
            builder = builder.credential_path(cred_file);
            log::info!("GCS: using credentials_file from config: {}", cred_file);
        } else if let Some(loader) = gcs_auth::try_authorized_user_loader()? {
            // A refreshing loader, not a static `.token()`: opendal pins a
            // static token with a usize::MAX expiry, so exports longer than
            // the ~1h ADC token TTL would 401 mid-run, non-retryably.
            log::info!(
                "GCS: using ADC {} credentials as {} (access token auto-refreshes before expiry)",
                loader.credential_kind(),
                loader.principal()
            );
            builder = builder
                .disable_vm_metadata()
                .customized_token_loader(Box::new(loader));
        } else {
            log::info!(
                "GCS: using Google default credential chain \
                 (service account JSON via GOOGLE_APPLICATION_CREDENTIALS, then VM metadata)"
            );
        }

        Ok(Operator::new(builder)?.finish())
    }
}

/// `None` for an object deleted since it was listed; any other error stands.
fn unless_gone<T>(r: opendal::Result<T>) -> Result<Option<T>> {
    match r {
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
        other => Ok(Some(other?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The load store opens the destination's own backend, not GCS for every export.
    #[test]
    fn the_load_store_opens_each_destinations_own_backend() {
        use crate::config::DestinationType;
        let dest = |t: DestinationType, extra: DestinationConfig| DestinationConfig {
            destination_type: t,
            bucket: Some("b".into()),
            ..extra
        };
        unsafe { std::env::set_var("RIVET_OP_TEST_KEY", "a2V5") };
        let s3 = dest(
            DestinationType::S3,
            DestinationConfig {
                endpoint: Some("http://127.0.0.1:1".into()),
                region: Some("us-east-1".into()),
                ..Default::default()
            },
        );
        let az = dest(
            DestinationType::Azure,
            DestinationConfig {
                account_name: Some("a".into()),
                account_key_env: Some("RIVET_OP_TEST_KEY".into()),
                ..Default::default()
            },
        );
        let gcs = dest(
            DestinationType::Gcs,
            DestinationConfig {
                endpoint: Some("http://127.0.0.1:1".into()),
                allow_anonymous: true,
                ..Default::default()
            },
        );
        let scheme = |c: &DestinationConfig| operator_for(c).unwrap().info().scheme().to_string();
        assert_eq!(scheme(&s3), "s3");
        assert_eq!(scheme(&az), "azblob");
        assert_eq!(scheme(&gcs), "gcs");
    }

    #[test]
    fn read_each_within_keeps_order_and_never_reads_an_object_over_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("small"), b"abc").unwrap();
        std::fs::write(dir.path().join("big"), vec![b'x'; 100]).unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let paths = ["big".to_string(), "small".to_string()];
        let got = store
            .read_each_within(&paths, 10, |p, body| {
                Ok((p.to_string(), body.map(|b| b.len())))
            })
            .unwrap();
        assert_eq!(got, vec![("big".into(), Err(100)), ("small".into(), Ok(3))]);
        let none = store
            .read_each_within(&[], 10, |_, b| Ok(b.is_ok()))
            .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn an_object_exactly_at_the_cap_is_read_whole() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("edge"), vec![b'x'; 10]).unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let got = store
            .read_each_within(&["edge".to_string()], 10, |_, body| {
                Ok(body.map(|b| b.len()))
            })
            .unwrap();
        assert_eq!(got, vec![Ok(10)]);
    }

    /// Only NotFound means "deleted since it was listed"; any other stat or open error fails the read.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_object_is_an_error_not_a_skip() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sealed = dir.path().join("sealed");
        std::fs::create_dir(&sealed).unwrap();
        std::fs::write(sealed.join("m"), b"abc").unwrap();
        std::fs::write(dir.path().join("locked"), b"abc").unwrap();
        let set = |p: &std::path::Path, mode| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap()
        };
        set(&sealed, 0o000);
        set(&dir.path().join("locked"), 0o000);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let read = |p: &str| store.read_each_within(&[p.to_string()], 10, |_, b| Ok(b.is_ok()));
        let (stat_denied, open_denied) = (read("sealed/m"), read("locked"));
        set(&sealed, 0o755);
        set(&dir.path().join("locked"), 0o644);
        assert!(
            stat_denied.is_err(),
            "a stat that fails for another reason: {stat_denied:?}"
        );
        assert!(
            open_denied.is_err(),
            "an open that fails for another reason: {open_denied:?}"
        );
    }

    /// A key listed and then deleted (a retired running marker) is skipped, not fatal.
    #[test]
    fn read_each_within_skips_a_path_deleted_after_it_was_listed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("kept"), b"abc").unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let paths = ["gone".to_string(), "kept".to_string()];
        let got = store
            .read_each_within(&paths, 10, |p, body| Ok((p.to_string(), body.is_ok())))
            .unwrap();
        assert_eq!(got, vec![("kept".into(), true)]);
    }

    /// The streamed read's own cap — the guard for an object that grew between the stat
    /// and the read (the stat branch is covered above). A revert to reading exactly the
    /// stat's size cannot be exercised here: the fs backend cannot swap an object between
    /// the two calls.
    #[test]
    fn a_body_that_grows_past_the_cap_while_streaming_is_refused() {
        let mut buf = Vec::new();
        assert_eq!(push_within(&mut buf, b"abc", 5), None);
        assert_eq!(
            push_within(&mut buf, b"de", 5),
            None,
            "exactly the cap still fits"
        );
        assert_eq!(
            push_within(&mut buf, b"f", 5),
            Some(6),
            "one byte over is refused"
        );
    }

    #[test]
    fn every_store_drives_its_operator_on_the_one_process_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let a = GcsStore::open_fs(root).unwrap();
        let b = GcsStore::open_fs(root).unwrap();
        assert!(
            Arc::ptr_eq(a.runtime(), b.runtime()),
            "a runtime per store drops pooled HTTP connections another worker still uses"
        );
        drop(a);
        b.list_files("")
            .expect("a dropped sibling store must not take the runtime with it");
    }

    /// Write `bytes` to `root/rel`, creating parent dirs — a stand-in for objects
    /// landing under a bucket prefix.
    fn write_at(root: &std::path::Path, rel: &str, bytes: &[u8]) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    #[test]
    fn list_files_is_recursive_file_only_and_bucket_relative() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "base/a.parquet", b"a");
        write_at(root, "base/b.parquet", b"b");
        write_at(root, "base/sub/c.parquet", b"c");
        write_at(root, "base/manifest.json", b"{}");
        write_at(root, "other/d.parquet", b"d"); // outside `base` — must not appear

        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();
        let mut got = store.list_files("base").unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                "base/a.parquet".to_string(),
                "base/b.parquet".to_string(),
                "base/manifest.json".to_string(),
                "base/sub/c.parquet".to_string(),
            ],
            "every file under the prefix, recursively, keyed bucket-relative — dirs excluded, siblings excluded"
        );

        // Real callers pass a trailing-slash prefix (`gs://bucket/base/`); it must
        // list identically. Pins the `is_empty() || ends_with('/')` guard — an
        // `&&` there would append a second slash (`base//`) and match nothing.
        let mut with_slash = store.list_files("base/").unwrap();
        with_slash.sort();
        assert_eq!(
            with_slash, got,
            "a trailing-slash prefix lists the same files"
        );
    }

    #[test]
    fn read_returns_the_object_bytes() {
        let dir = tempfile::tempdir().unwrap();
        write_at(dir.path(), "p/hello.bin", b"payload");
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(store.read("p/hello.bin").unwrap(), b"payload");
    }

    #[test]
    fn read_range_returns_the_requested_slice() {
        let dir = tempfile::tempdir().unwrap();
        write_at(dir.path(), "exports/a.bin", b"0123456789");
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(store.read_range("exports/a.bin", 3, 4).unwrap(), b"3456");
        assert_eq!(store.read_range("exports/a.bin", 8, 2).unwrap(), b"89");
    }

    #[test]
    fn remove_deletes_one_object_and_missing_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "p/a.parquet", b"a");
        write_at(root, "p/b.parquet", b"b");
        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();

        store.remove("p/a.parquet").unwrap();
        assert_eq!(
            store.list_files("p").unwrap(),
            vec!["p/b.parquet".to_string()],
            "only the named object is gone; its sibling survives"
        );
        // A crash/retry can call remove on an already-gone key — must be Ok.
        store.remove("p/a.parquet").unwrap();
    }

    #[test]
    fn stat_size_reports_the_object_length() {
        let dir = tempfile::tempdir().unwrap();
        write_at(dir.path(), "p/a.parquet", b"abcd"); // 4 bytes
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(store.stat_size("p/a.parquet").unwrap(), 4);
    }

    // RED before dir_boundary in remove_all: opendal matches by STRING prefix,
    // so `remove_all("p")` (no trailing slash — exactly what the load cleanup
    // passes for `prefix: "exports/orders"` or a mid-segment `{partition}`) also
    // deletes `p_archive/…`, a SEPARATE sibling export. Data destruction, and
    // reproduced on the fs backend (opendal string-prefixes there too). The
    // prior test used `keep/` — not a string prefix — so it never activated it.
    #[test]
    fn remove_all_spares_a_string_prefix_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "p/a.parquet", b"a");
        write_at(root, "p_archive/b.parquet", b"b"); // key string-starts-with "p"
        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();

        store.remove_all("p").unwrap();
        assert!(
            store.list_files("p").unwrap().is_empty(),
            "the target subtree is drained"
        );
        assert_eq!(
            store.list_files("p_archive").unwrap(),
            vec!["p_archive/b.parquet".to_string()],
            "a SEPARATE export sharing the string prefix must NOT be deleted"
        );
    }

    #[test]
    fn remove_all_recursively_empties_the_prefix_and_spares_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "p/a.parquet", b"a");
        write_at(root, "p/sub/b.parquet", b"b");
        write_at(root, "keep/c.parquet", b"c");
        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();

        // `delete_under` passes the bucket-relative prefix with no trailing
        // slash — the recursive delete must still drain the whole subtree.
        store.remove_all("p").unwrap();
        assert!(
            store.list_files("p").unwrap().is_empty(),
            "the prefix subtree is fully drained"
        );
        assert_eq!(
            store.list_files("keep").unwrap(),
            vec!["keep/c.parquet".to_string()],
            "objects outside the prefix are untouched"
        );
    }

    #[test]
    fn remove_all_on_a_missing_prefix_is_a_no_op_not_an_error() {
        // `cleanup_source` runs after a load; a retried load (or a crash between
        // cleanup and the next run) can call it on an ALREADY-empty prefix. That
        // must be a no-op `Ok(())`, never an error that fails the whole load.
        let dir = tempfile::tempdir().unwrap();
        write_at(dir.path(), "keep/c.parquet", b"c"); // a sibling, untouched
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        store
            .remove_all("never/existed")
            .expect("deleting a nonexistent prefix must be a no-op");
        assert_eq!(
            store.list_files("keep").unwrap(),
            vec!["keep/c.parquet".to_string()],
            "a no-op delete touches nothing"
        );
    }

    #[test]
    fn list_files_on_a_missing_prefix_is_empty_not_an_error() {
        // reconcile/plan list before they know a prefix has anything; a missing
        // prefix must read as empty, not blow up the load.
        let dir = tempfile::tempdir().unwrap();
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        assert!(
            store
                .list_files("no/such/prefix")
                .expect("listing a missing prefix must succeed")
                .is_empty()
        );
    }
}
