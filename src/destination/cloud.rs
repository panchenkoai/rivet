//! **Layer: Execution** — shared base for OpenDAL-backed cloud destinations.
//!
//! S3, GCS, and Azure differ only in how they *build* their OpenDAL operator
//! (bucket + region + STS creds vs container + SAS/account-key vs
//! service-account auth) and in the URI scheme they log. Everything that
//! happens *after* the operator exists is byte-identical across the three:
//! the [`RetryLayer`] policy, the blocking-operator wrap, the keeps-the-tokio-
//! runtime-alive `Arc`, the `prefix` join, and the entire ADR-0013 read
//! surface (`write` / `list_prefix` / `read` / `head` / `move`).
//!
//! Before this module each backend hand-rolled that tail, so a fix to the
//! listing trailing-slash rule or the move copy+delete fallback meant three
//! edits that could drift. [`CloudBackend`] is the seam: a backend supplies
//! `build_operator` plus two consts; [`CloudDestination`] owns the rest. A
//! new object-store backend is now "implement `build_operator`" — not "copy
//! 120 lines of read surface and hope they stay in sync".
//!
//! The local filesystem destination is deliberately *not* expressed here: it
//! is not OpenDAL-backed and has genuinely different semantics (no runtime,
//! `fs::copy` partial-write risk, depth-first walk). Forcing it through this
//! seam would be a shallow abstraction.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;

use crate::config::DestinationConfig;
use crate::error::Result;

/// A backend's contribution to a cloud destination: how to build its OpenDAL
/// operator and how to name itself in logs/errors. Everything else lives in
/// [`CloudDestination`].
pub(crate) trait CloudBackend {
    /// Backend label interpolated into the tokio-runtime construction error
    /// (`"S3"`, `"GCS"`, `"Azure"`).
    const RUNTIME_LABEL: &'static str;
    /// URI scheme logged after a successful upload (`"s3"`, `"gs"`, `"az"`).
    const SCHEME: &'static str;

    /// Build the configured, **un-layered** async operator from `config`.
    ///
    /// Called inside the destination's tokio runtime guard, so backend auth
    /// preflight (e.g. Azure SAS-expiry enforcement) and `Operator::new`
    /// both run with a runtime in context — same ordering the per-backend
    /// `new()` functions used before this seam existed. The shared
    /// [`RetryLayer`] is applied by [`CloudDestination::new`], so backends
    /// must return the operator *without* their own retry layer.
    fn build_operator(config: &DestinationConfig) -> Result<Operator>;
}

/// OpenDAL-backed object-store destination, generic over the backend `B`.
///
/// Object-safe `Destination` is implemented once here for every `B`, so S3,
/// GCS, and Azure share one copy of the retry policy, the prefix join, and
/// the ADR-0013 read surface. `B` is a zero-sized marker; `PhantomData<fn()
/// -> B>` keeps `CloudDestination<B>: Send + Sync` without constraining `B`.
pub(crate) struct CloudDestination<B: CloudBackend> {
    // Held so the runtime outlives the blocking operator that drives it.
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
    prefix: String,
    _backend: PhantomData<fn() -> B>,
}

impl<B: CloudBackend> CloudDestination<B> {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "failed to create tokio runtime for {}: {}",
                        B::RUNTIME_LABEL,
                        e
                    )
                })?,
        );
        let _guard = runtime.enter();

        // OpenDAL's `RetryLayer` retries individual HTTP calls on hyper /
        // reqwest transient failures (`dispatch task is gone`, server-side
        // 5xx, 429, partial-upload disconnects, …) without re-running the
        // whole chunk through the source. The chunk worker's outer retry
        // loop is still the safety net for harder failures (auth, region,
        // SQL retries) — this just stops a single TCP blip from poisoning a
        // streaming upload that otherwise costs another full SQL fetch +
        // parquet encode. One policy, applied identically to every backend.
        let async_op = B::build_operator(config)?.layer(
            RetryLayer::new()
                .with_max_times(5)
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        );
        let op = blocking::Operator::new(async_op)?;

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
            _backend: PhantomData,
        })
    }
}

impl<B: CloudBackend> super::Destination for CloudDestination<B> {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let size = std::fs::metadata(local_path)?.len();
        // One-shot upload for parts that fit comfortably in memory: a single
        // PUT (S3 `PutObject` / GCS upload / Azure `Put Blob`) makes the store
        // compute and store a content checksum that the listing then exposes
        // for no-download verification.  This is what lets `--validate`
        // md5-check Azure parts at all: Azure auto-computes `Content-MD5` only
        // for a single `Put Blob`, never for the `Put Block List` that the
        // streaming writer produces (each `write()` call past the first stages
        // a block).  Above the threshold we stream — bounding memory at the
        // cost of size-only verification for those (large) parts.
        const ONE_SHOT_MAX: u64 = 256 * 1024 * 1024;
        if size <= ONE_SHOT_MAX {
            let body = std::fs::read(local_path)?;
            self.op.write(&key, body)?;
        } else {
            let mut src = std::fs::File::open(local_path)?;
            let mut dst = self.op.writer(&key)?.into_std_write();
            std::io::copy(&mut src, &mut dst)?;
            dst.close()?;
        }
        log::info!("uploaded {}://{} ({size} bytes)", B::SCHEME, key);
        Ok(())
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::FinalizeOnClose,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }

    // ── ADR-0013 read surface (delegates to opendal) ─────────────────────
    //
    // opendal abstracts the backend-specific listing / stat semantics, so
    // these are identical for every object store. The `prefix` arg is
    // configured-prefix-relative; we apply the same `self.prefix` join the
    // writer applies so callers see a consistent namespace. Returned `key`
    // values are *also* configured-prefix-relative — symmetric with
    // `write`'s `remote_key` argument.

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, prefix);
        // opendal expects a trailing `/` for directory listings. For a
        // bucket/container root the empty string is fine; for any non-empty
        // prefix we add `/` if the caller didn't.
        let listed = if full.is_empty() || full.ends_with('/') {
            self.op.list_options(
                &full,
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        } else {
            self.op.list_options(
                &format!("{}/", full),
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        };
        let mut out = Vec::with_capacity(listed.len());
        for entry in listed {
            if entry.metadata().mode() != opendal::EntryMode::FILE {
                continue;
            }
            // entry.path() returns a bucket-root-absolute key; strip our
            // configured prefix so the returned `key` is comparable to
            // values the caller passed to `write`.
            let abs = entry.path().to_string();
            let rel = abs
                .strip_prefix(self.prefix.as_str())
                .unwrap_or(abs.as_str())
                .to_string();
            out.push(super::ObjectMeta {
                key: rel,
                size_bytes: entry.metadata().content_length(),
                content_md5: entry.metadata().content_md5().map(str::to_string),
            });
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let full = format!("{}{}", self.prefix, key);
        let buf = self.op.read(&full)?;
        Ok(buf.to_vec())
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, key);
        // `stat` returns NotFound for absent keys; opendal exposes the
        // discriminator on the returned error so we can keep our contract
        // "Ok(None) is unambiguous absence".
        match self.op.stat(&full) {
            Ok(meta) => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: meta.content_length(),
                content_md5: meta.content_md5().map(str::to_string),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        // Object stores are not POSIX — no native rename. opendal 0.55
        // returns `Unsupported` for `rename` on S3 / GCS / Azure Blob, so we
        // do it ourselves: server-side copy + delete. ADR-0012 M9
        // best-effort: a partial copy-ok / delete-fail leaves the source
        // reachable at both paths and re-trips M9 on the next resume —
        // a clutter problem, not a correctness one.
        let from_full = format!("{}{}", self.prefix, from);
        let to_full = format!("{}{}", self.prefix, to);
        self.op.copy(&from_full, &to_full)?;
        self.op.delete(&from_full)?;
        Ok(())
    }
}
