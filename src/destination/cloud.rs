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
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;

use crate::config::DestinationConfig;
use crate::error::Result;

/// Process-wide ceiling on RAM held in one-shot upload buffers.
///
/// A single-PUT upload (`op.write`) must buffer the whole part so the store
/// computes and stores a content MD5 the listing exposes (the only way to get
/// `Content-MD5` on Azure — a single `Put Blob`, not `Put Block List`).  That
/// buffering is unavoidable, so the risk is buffer × upload concurrency
/// (`parallel`, default 4, operator-tunable).  Rather than a per-part magic
/// threshold that still multiplies by concurrency, a part one-shots only if it
/// fits in the *remaining* shared budget; otherwise it streams (memory-bounded,
/// size-only verification).  Total one-shot RAM is thus capped here regardless
/// of how many workers upload at once, and any part larger than the whole
/// budget always streams.
const ONESHOT_BUDGET_BYTES: i64 = 64 * 1024 * 1024;
static ONESHOT_BUDGET: AtomicI64 = AtomicI64::new(ONESHOT_BUDGET_BYTES);

/// Releases the reserved bytes back to [`ONESHOT_BUDGET`] on drop — so the
/// budget is restored even if the upload errors out.
struct OneShotReservation(i64);
impl Drop for OneShotReservation {
    fn drop(&mut self) {
        ONESHOT_BUDGET.fetch_add(self.0, Ordering::Relaxed);
    }
}

/// Reserve `size` bytes for a one-shot buffer if the budget allows, else `None`
/// (caller streams).  Parts larger than the whole budget never fit, so they
/// always stream.
fn reserve_oneshot(size: u64) -> Option<OneShotReservation> {
    let size = i64::try_from(size).unwrap_or(i64::MAX);
    take_from(&ONESHOT_BUDGET, size).then_some(OneShotReservation(size))
}

/// Optimistic atomic reserve: subtract `size`; if that would overdraw, undo and
/// fail.  Concurrency-safe — a transient negative from a racing subtract just
/// makes one caller stream (a benign false-negative), never an overdraw.
fn take_from(budget: &AtomicI64, size: i64) -> bool {
    if budget.fetch_sub(size, Ordering::Relaxed) >= size {
        true
    } else {
        budget.fetch_add(size, Ordering::Relaxed);
        false
    }
}

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
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        let key = format!("{}{}", self.prefix, remote_key);
        let size = std::fs::metadata(local_path)?.len();
        // One-shot upload when the part fits the shared memory budget: a single
        // PUT (S3 `PutObject` / GCS upload / Azure `Put Blob`) makes the store
        // compute and store a content checksum the listing then exposes for
        // no-download verification.  This is what lets `--validate` md5-check
        // Azure parts at all — Azure auto-computes `Content-MD5` only for a
        // single `Put Blob`, never for the `Put Block List` the streaming
        // writer produces (each `write()` past the first stages a block).
        // Otherwise stream — memory-bounded, size-only for those parts.
        let outcome = if let Some(_reservation) = reserve_oneshot(size) {
            let body = std::fs::read(local_path)?;
            let meta = self.op.write(&key, body)?;
            // The single-PUT response carries the store's own checksum: GCS /
            // Azure as `content_md5` (base64), S3 as the ETag (hex MD5).  Hand
            // it back for the commit-time transit check.
            super::WriteOutcome {
                content_md5: meta
                    .content_md5()
                    .map(str::to_string)
                    .or_else(|| meta.etag().map(|e| e.trim_matches('"').to_string())),
            }
        } else {
            let mut src = std::fs::File::open(local_path)?;
            let mut dst = self.op.writer(&key)?.into_std_write();
            std::io::copy(&mut src, &mut dst)?;
            dst.close()?;
            // Streamed (multipart / block-list): no full-object checksum.
            super::WriteOutcome::opaque()
        };
        log::info!("uploaded {}://{} ({size} bytes)", B::SCHEME, key);
        Ok(outcome)
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

#[cfg(test)]
mod tests {
    use super::{AtomicI64, Ordering, take_from};

    #[test]
    fn oneshot_budget_reserves_until_exhausted_then_streams() {
        let budget = AtomicI64::new(100);
        // Two parts that fit are reserved; the third overdraws and streams.
        assert!(take_from(&budget, 60), "first fits");
        assert!(take_from(&budget, 30), "second fits (10 left)");
        assert!(!take_from(&budget, 30), "third overdraws → stream");
        // The failed reservation must NOT have consumed budget.
        assert_eq!(
            budget.load(Ordering::Relaxed),
            10,
            "budget intact after overdraw"
        );
        // Releasing the 60-byte reservation frees it for the next part.
        budget.fetch_add(60, Ordering::Relaxed);
        assert!(take_from(&budget, 30), "fits again after release");
    }

    #[test]
    fn part_larger_than_whole_budget_never_reserves() {
        let budget = AtomicI64::new(64);
        assert!(
            !take_from(&budget, 1_000),
            "part bigger than budget streams"
        );
        assert_eq!(budget.load(Ordering::Relaxed), 64, "budget untouched");
    }
}
