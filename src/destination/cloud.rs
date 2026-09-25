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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;

use crate::config::DestinationConfig;
use crate::error::Result;

/// Process-wide count of transient destination-side retry ATTEMPTS the
/// [`RetryLayer`] scheduled. Read by the run summary so the "destination was
/// flaky today" signal survives even though the per-attempt log lines are
/// demoted to DEBUG after the first.
///
/// **It counts attempts, not recoveries** — see [`transient_retries_summary`]
/// for why the interceptor cannot know which retries then succeeded.
pub(crate) static TRANSIENT_RETRIES: AtomicU64 = AtomicU64::new(0);

/// Total transient destination retry attempts made so far in this process.
pub fn transient_retries_total() -> u64 {
    TRANSIENT_RETRIES.load(Ordering::Relaxed)
}

/// The run-summary value for `n` transient destination retries — `None` when
/// there were none (the caller omits the row entirely).
///
/// **Says "attempts", never "all recovered".** opendal's
/// [`opendal::layers::RetryInterceptor`] contract fires the interceptor
/// "just before the retry sleep" with only `(err, dur)` and no return
/// channel, and backon notifies only on the way INTO another attempt (a
/// budget-exhausting failure returns without a final notify) — so the
/// interceptor never learns whether the attempt it announced then succeeded.
/// The last counted retry may be exactly the one whose attempt failed the
/// run. The old wording ("N transient (all recovered)") therefore made its
/// strongest claim on precisely the run where it was false: one that died on
/// a destination error.
pub fn transient_retries_summary(n: u64) -> Option<String> {
    (n > 0).then(|| format!("{n} transient retry attempts (RUST_LOG=debug for detail)"))
}

/// Fold a CHILD process's retry total into this process's counter — the
/// parent of `--parallel-export-processes` / wave-parallel children calls
/// this from the Finished-event handler so the run summary's "dest retries"
/// line covers the whole run, not just the parent's own probes.
pub fn add_transient_retries(n: u64) {
    TRANSIENT_RETRIES.fetch_add(n, Ordering::Relaxed);
}

/// Rivet's [`opendal::layers::RetryInterceptor`]: keep the truth, drop the
/// spam. The FIRST transient retry in the process logs at WARN with the
/// error kind (so an operator sees what the destination is doing), every
/// subsequent one logs at DEBUG, and all of them are counted for the run
/// summary. The count is of retry ATTEMPTS — this hook runs before the
/// retry sleep and is never told the outcome, so it cannot claim recovery
/// (see [`transient_retries_summary`]). Replacing opendal's default per-attempt WARN — which on a busy
/// parallel upload was every other log line (field find, 2026-08-13) —
/// with a log-filter mute would have hidden the degradation signal
/// entirely; this aggregates it instead.
pub(crate) struct RivetRetryNotify;

impl opendal::layers::RetryInterceptor for RivetRetryNotify {
    fn intercept(&self, err: &opendal::Error, dur: Duration) {
        let n = TRANSIENT_RETRIES.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            log::warn!(
                "destination: transient error, retrying in {:.1}s (further retries log at \
                 DEBUG; the run summary reports the total): {err}",
                dur.as_secs_f64(),
            );
        } else {
            log::debug!(
                "destination: transient error, retrying in {:.1}s: {err}",
                dur.as_secs_f64(),
            );
        }
    }
}

/// Default ceiling on RAM held in one-shot upload buffers.
///
/// A single-PUT upload (`op.write`) must buffer the whole part so the store
/// computes and stores a content MD5 the listing exposes (the only way to get
/// `Content-MD5` on Azure — a single `Put Blob`, not `Put Block List`).  That
/// buffering is unavoidable, so the risk is buffer × upload concurrency.  A part
/// one-shots only if it fits in the *remaining* budget; otherwise it streams
/// (memory-bounded, size-only verification), so total one-shot RAM is capped
/// regardless of how many workers upload at once.
const DEFAULT_ONESHOT_BUDGET_MB: u64 = 64;

/// The process-wide one-shot pool for a budget size, shared by every destination configured with it.
///
/// Keyed by the configured `oneshot_budget_mb` (unset ≡ 64), never by the
/// destination instance: one CDC export builds a destination PER TABLE and its
/// roll uploads many tables at once, so a per-instance pool would turn one
/// config line into tables × budget.  Distinct values are few, so the leak is bounded.
fn oneshot_pool(config: &DestinationConfig) -> &'static AtomicI64 {
    static POOLS: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<u64, &'static AtomicI64>>,
    > = std::sync::LazyLock::new(Default::default);
    let mb = config
        .oneshot_budget_mb
        .unwrap_or(DEFAULT_ONESHOT_BUDGET_MB);
    let mut pools = POOLS.lock().unwrap_or_else(|e| e.into_inner());
    pools.entry(mb).or_insert_with(|| {
        let bytes = i64::try_from(mb)
            .unwrap_or(i64::MAX)
            .saturating_mul(1024 * 1024);
        Box::leak(Box::new(AtomicI64::new(bytes)))
    })
}

/// Releases the reserved bytes back to the pool it was taken from on drop — so
/// the budget is restored even if the upload errors out.
struct OneShotReservation<'a>(&'a AtomicI64, i64);
impl Drop for OneShotReservation<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(self.1, Ordering::Relaxed);
    }
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

    /// Is the `content_md5` a LISTING reports actually the object's MD5?
    ///
    /// `false` on S3, and the write path already knew why: opendal's S3 lister
    /// ALIASES the ETag into `content_md5` (`set_etag(etag);
    /// set_content_md5(etag.trim_matches('"'))`), while its S3 writer does not. The
    /// comment beside the single-PUT upload spells the consequence out — "for an
    /// SSE-KMS / SSE-C object the ETag is NOT the object's MD5" — and that reasoning
    /// was applied to `write` and never to `list_prefix`.
    ///
    /// The cost of the omission is not cosmetic: the manifest side is a locally
    /// computed MD5, `md5_digest_bytes` parses both encodings, so on an unencrypted
    /// single-part object they match by luck — and on a bucket with default
    /// encryption `aws:kms` they cannot. Every part then yields `ChecksumMismatch`:
    /// validate hard-fails on correct data, and the M8 resume quarantines each
    /// object out of the delivered prefix and re-exports it. Multipart composite
    /// ETags (`<hash>-<N>`) already degrade to size-only correctly; it is the
    /// single-part shape, still 32 hex characters, that lies convincingly.
    ///
    /// Round-11 bughunt, read-only (no KMS bucket here) — but the aliasing, the
    /// write-path refusal and the comparison were each read at their sites.
    const LIST_MD5_IS_TRUSTWORTHY: bool = true;
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
    /// The one-shot upload pool this destination draws from (see [`oneshot_pool`]).
    oneshot_budget: &'static AtomicI64,
    _backend: PhantomData<fn() -> B>,
}

/// Default retry budget for real exports: OpenDAL retries individual HTTP
/// calls this many times before giving up to the chunk worker's outer loop.
const DEFAULT_MAX_RETRIES: usize = 5;

/// Normalize a destination prefix to the object-store trailing-slash convention.
/// Every op builds a key as `format!("{}{}", self.prefix, key)`, so a non-empty
/// prefix WITHOUT a trailing slash jams the part name onto it (`exports/mydata` +
/// `orders.parquet` -> `exports/mydataorders.parquet`) while `list_prefix` appends
/// `/` and lists an empty `exports/mydata/` -> a false PART_MISSING on present data
/// (dogfood). Empty (bucket root) and already-slashed prefixes are unchanged.
fn normalize_prefix(p: String) -> String {
    // A LEADING slash is stripped too, and the trailing-slash comment above is the
    // reason to expect its sibling: opendal normalizes `/lead/x/` away on the wire,
    // so the data lands correctly at `lead/x/…` — but `self.prefix` keeps the slash,
    // and `list_prefix`'s `strip_prefix(self.prefix)` then FAILS and falls through to
    // the bucket-absolute key. MEASURED on MinIO with `prefix: "/lead/x/"`: `rivet
    // validate` reported the SAME object as `PART_MISSING` and as an
    // `UNTRACKED_OBJECT` in one run, on 100%-correct data. Every listing consumer
    // shares this — validate, the M8 resume decisions (every part → Rewrite, every
    // real object → Quarantine), split-unit manifest reads, the CDC validator and
    // the value-checksum re-read.
    let p = p.trim_start_matches('/').to_string();
    if p.is_empty() || p.ends_with('/') {
        p
    } else {
        format!("{p}/")
    }
}

/// The one tokio runtime every object-store operator runs on: opendal sends through a process-global HTTP pool, and a pooled connection dies with the runtime that opened it.
pub(crate) fn io_runtime() -> Result<Arc<tokio::runtime::Runtime>> {
    static RUNTIME: std::sync::OnceLock<Arc<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    if let Some(rt) = RUNTIME.get() {
        return Ok(Arc::clone(rt));
    }
    let built = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("rivet-io")
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("failed to create the object-store tokio runtime: {e}"))?,
    );
    Ok(Arc::clone(RUNTIME.get_or_init(|| built)))
}

impl<B: CloudBackend> CloudDestination<B> {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        Self::new_with_retries(config, DEFAULT_MAX_RETRIES)
    }

    /// Build the destination with an explicit OpenDAL retry budget.
    ///
    /// Real exports use [`new`] (`DEFAULT_MAX_RETRIES` = 5). A preflight
    /// connectivity probe (`rivet doctor`) wants to FAIL FAST against an
    /// unreachable endpoint rather than inherit the export's ~10s of
    /// escalating-backoff retries, so it passes `max_times = 0`: with a zero
    /// budget OpenDAL's `RetryLayer` makes a single attempt and surfaces the
    /// transport error immediately. Default (export) behavior is unchanged —
    /// `new` still threads 5 here.
    pub fn new_with_retries(config: &DestinationConfig, max_times: usize) -> Result<Self> {
        let runtime = io_runtime()?;
        let _guard = runtime.enter();

        // OpenDAL's `RetryLayer` retries individual HTTP calls on hyper /
        // reqwest transient failures (`dispatch task is gone`, server-side
        // 5xx, 429, partial-upload disconnects, …) without re-running the
        // whole chunk through the source. The chunk worker's outer retry
        // loop is still the safety net for harder failures (auth, region,
        // SQL retries) — this just stops a single TCP blip from poisoning a
        // streaming upload that otherwise costs another full SQL fetch +
        // parquet encode. One policy, applied identically to every backend.
        // `max_times == 0` disables retries entirely (single attempt) — the
        // fail-fast path the doctor probe wants.
        let async_op = B::build_operator(config)?.layer(
            RetryLayer::new()
                .with_max_times(max_times)
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(10))
                .with_jitter()
                .with_notify(RivetRetryNotify),
        );
        let op = blocking::Operator::new(async_op)?;

        // Normalize the prefix to a trailing `/` (object-store convention). Every
        // op builds a key as `format!("{}{}", self.prefix, key)`, so a prefix
        // WITHOUT a trailing slash JAMS the part name onto it — `write` stores
        // `exports/mydataorders_….parquet` while `list_prefix` appends `/` and
        // lists `exports/mydata/` (empty) → a false PART_MISSING on 100%-present,
        // DuckDB-readable data (dogfood: a natural `prefix: exports/mydata` form
        // that rivet silently accepted then failed to verify). Adding the slash
        // here makes write/list/read/head agree; a prefix that already ends in `/`
        // (or is empty = bucket root) is unchanged, so existing configs are inert.
        let prefix = normalize_prefix(config.prefix.clone().unwrap_or_default());

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
            oneshot_budget: oneshot_pool(config),
            _backend: PhantomData,
        })
    }

    /// Reserve `size` bytes for a one-shot buffer from this destination's
    /// budget if it allows, else `None` (caller streams).  Parts larger than
    /// the whole budget never fit, so they always stream.
    fn reserve_oneshot(&self, size: u64) -> Option<OneShotReservation<'_>> {
        let size = i64::try_from(size).unwrap_or(i64::MAX);
        take_from(self.oneshot_budget, size)
            .then_some(OneShotReservation(self.oneshot_budget, size))
    }
}

impl<B: CloudBackend> super::Destination for CloudDestination<B> {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        let key = format!("{}{}", self.prefix, remote_key);
        let size = std::fs::metadata(local_path)?.len();
        // One-shot upload when the part fits this destination's memory budget: one
        // PUT instead of a sequential multipart (5 MiB parts on S3/GCS, 256 KiB
        // blocks on Azure), and on GCS / Azure a store-computed Content-MD5 that
        // `--validate` checks with no download (Azure computes it only for a single
        // `Put Blob`). Otherwise stream — memory-bounded, size-only for those parts.
        let outcome = if let Some(_reservation) = self.reserve_oneshot(size) {
            let body = std::fs::read(local_path)?;
            let meta = self.op.write(&key, body)?;
            // The single-PUT response carries the store's own checksum on GCS /
            // Azure (`content_md5`, base64); hand it back for the transit check.
            super::WriteOutcome {
                // Use the store's REAL Content-MD5 header only (GCS / Azure
                // return it, base64). Do NOT fall back to the S3 ETag: for an
                // SSE-KMS / SSE-C object the ETag is NOT the object's MD5 (AWS
                // documents this) yet is still a 32-hex string, so trusting it as
                // an MD5 oracle mis-verifies the transit on any bucket with
                // default encryption (bug hunt 2026-08-09). `None` here means the
                // commit-time transit check is skipped for that part — the size
                // check still runs — rather than checked against a wrong digest.
                content_md5: meta.content_md5().map(str::to_string),
            }
        } else {
            let mut src = std::fs::File::open(local_path)?;
            let mut dst = self.op.writer(&key)?.into_std_write();
            // KNOWN LEAK on the error path (round-4, MED): a copy/close failure
            // propagates without aborting the multipart upload — opendal's
            // BLOCKING writer exposes no `abort()` (the async one does), so the
            // orphaned upload's parts stay billed-but-invisible until the
            // bucket's lifecycle rule reaps them. Two halves to the fix, one
            // shipped: (1) docs require an incomplete-multipart lifecycle rule
            // on every bucket (load-bearing regardless — an abort call itself
            // dies with the process on a crash); (2) an explicit abort needs
            // this branch moved onto the ASYNC writer driven via the runtime —
            // tracked, not attempted inline on the hot upload path.
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
                // Dropped for a backend whose listing aliases something else into
                // this field — see `LIST_MD5_IS_TRUSTWORTHY`. Size-only is the
                // honest degrade; a wrong digest is worse than no digest, because
                // the consumers treat a mismatch as corruption.
                content_md5: if B::LIST_MD5_IS_TRUSTWORTHY {
                    entry.metadata().content_md5().map(str::to_string)
                } else {
                    None
                },
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

    fn remove(&self, key: &str) -> Result<()> {
        self.op.delete(&format!("{}{}", self.prefix, key))?;
        Ok(())
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
    use super::{
        AtomicI64, CloudDestination, OneShotReservation, Ordering, normalize_prefix, oneshot_pool,
        take_from,
    };
    /// The RELEASE half: a finished reservation must return its bytes, or the pool
    /// drains for the life of the process and every later part silently streams.
    #[test]
    fn a_finished_reservation_returns_its_bytes_so_the_next_part_can_one_shot() {
        let pool = AtomicI64::new(8 * 1024 * 1024);
        let whole = 8 * 1024 * 1024;

        assert!(
            take_from(&pool, whole),
            "the whole budget is available to start with"
        );
        {
            let _held = OneShotReservation(&pool, whole);
            assert!(
                !take_from(&pool, whole),
                "while the whole budget is held, a second part of the same size must stream"
            );
        }
        assert!(
            take_from(&pool, whole),
            "after the first reservation finished, the next part must one-shot again"
        );
    }

    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::gcs::GcsBackend;

    #[test]
    fn normalize_prefix_appends_a_trailing_slash_only_when_needed() {
        // dogfood HIGH-adjacent: a no-slash prefix jammed the part name and broke
        // list (false PART_MISSING). Normalization must add exactly one slash to a
        // non-empty, non-slashed prefix, and leave the other two forms untouched.
        assert_eq!(normalize_prefix("exports/mydata".into()), "exports/mydata/");
        assert_eq!(
            normalize_prefix("exports/mydata/".into()),
            "exports/mydata/"
        );
        assert_eq!(normalize_prefix(String::new()), ""); // bucket root
        assert_eq!(normalize_prefix("a".into()), "a/");
        // The LEADING slash, whose absence made `list_prefix` return
        // bucket-absolute keys: `strip_prefix("/lead/x/")` cannot match a key
        // opendal already normalized to `lead/x/…`, and the `unwrap_or` swallows
        // that. MEASURED on MinIO — the same object reported PART_MISSING and
        // UNTRACKED_OBJECT in one `rivet validate`, on correct data.
        assert_eq!(normalize_prefix("/lead/x/".into()), "lead/x/");
        assert_eq!(normalize_prefix("/lead/x".into()), "lead/x/");
        assert_eq!(
            normalize_prefix("/".into()),
            "",
            "a lone slash is the bucket ROOT, not a directory named empty"
        );
    }

    // L20 (cloud-fastfail): the no-retry probe seam must construct. A GCS
    // `allow_anonymous` config builds the OpenDAL operator without touching
    // the wire (Azurite/emulator path), so this exercises `new_with_retries`
    // end-to-end with `max_times = 0` — the value the doctor probe threads to
    // disable the export's 5-attempt escalating backoff. If `with_max_times`
    // ever rejected 0 (or the seam regressed), this construction would fail.
    #[test]
    fn new_with_retries_zero_builds_no_retry_probe_destination() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("rivet-fastfail-probe".into()),
            // Emulator/anonymous: skips OAuth, builds operator offline.
            allow_anonymous: true,
            endpoint: Some("http://127.0.0.1:4443".into()),
            ..Default::default()
        };
        // The construction itself is the assertion: a zero retry budget is a
        // valid `RetryLayer` config and the probe seam reaches it. (The built
        // `blocking::Operator` is opaque, so the retry count can't be read
        // back here — the live timing test in tests/audit_doctor_fastfail.rs
        // proves the *behavioral* fail-fast against a closed port.)
        CloudDestination::<GcsBackend>::new_with_retries(&cfg, 0)
            .expect("no-retry probe destination must build");
    }

    /// A GCS destination config with the given budget (no network is touched).
    fn gcs_cfg(budget_mb: Option<u64>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("rivet-oneshot-pool".into()),
            allow_anonymous: true,
            endpoint: Some("http://127.0.0.1:4443".into()),
            oneshot_budget_mb: budget_mb,
            ..Default::default()
        }
    }

    #[test]
    fn an_unset_budget_is_the_64mb_pool_and_a_value_sizes_its_own() {
        assert!(
            std::ptr::eq(
                oneshot_pool(&gcs_cfg(None)),
                oneshot_pool(&gcs_cfg(Some(64)))
            ),
            "unset must be the historical process-wide 64 MB pool"
        );
        assert_eq!(oneshot_pool(&gcs_cfg(Some(0))).load(Ordering::Relaxed), 0);
        assert_eq!(
            oneshot_pool(&gcs_cfg(Some(131))).load(Ordering::Relaxed),
            131 * 1024 * 1024
        );
    }

    #[test]
    fn configured_budget_moves_the_oneshot_switch_point() {
        let part = 40 * 1024 * 1024;
        let small =
            CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(33)), 0).unwrap();
        assert!(
            small.reserve_oneshot(part).is_none(),
            "a 40 MB part streams under a 33 MB budget"
        );
        let big = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(129)), 0).unwrap();
        assert!(
            big.reserve_oneshot(part).is_some(),
            "the same part one-shots under a 129 MB budget"
        );
    }

    /// One CDC export builds a destination per table: they must share ONE pool,
    /// or `oneshot_budget_mb` multiplies by the table count.
    #[test]
    fn destinations_with_the_same_budget_share_one_pool() {
        let whole = 67 * 1024 * 1024;
        let a = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(67)), 0).unwrap();
        let b = CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(67)), 0).unwrap();
        let other =
            CloudDestination::<GcsBackend>::new_with_retries(&gcs_cfg(Some(68)), 0).unwrap();

        let _held = a
            .reserve_oneshot(whole)
            .expect("a takes the whole 67 MB pool");
        assert!(
            b.reserve_oneshot(1).is_none(),
            "b shares a's drained pool: the budget must not multiply per destination"
        );
        assert!(
            other.reserve_oneshot(whole).is_some(),
            "a different budget value is a different pool"
        );
    }

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

    /// The retry interceptor must COUNT every transient retry it absorbs —
    /// the run summary reads this total, and it is the only place the
    /// "destination was flaky" signal survives once the per-attempt lines
    /// drop to DEBUG after the first. RED against an interceptor that logs
    /// without counting (the field-find alternative of muting the log
    /// target wholesale would have zeroed this signal entirely).
    #[test]
    fn retry_interceptor_counts_every_absorbed_retry() {
        use super::{RivetRetryNotify, transient_retries_total};
        use opendal::layers::RetryInterceptor as _;
        use std::time::Duration;
        let before = transient_retries_total();
        let err = opendal::Error::new(opendal::ErrorKind::Unexpected, "transient blip");
        RivetRetryNotify.intercept(&err, Duration::from_millis(200));
        RivetRetryNotify.intercept(&err, Duration::from_millis(400));
        // Delta, not absolute: the counter is process-wide and other tests may
        // run concurrently in this process.
        assert!(
            transient_retries_total() >= before + 2,
            "both retries must be counted"
        );
    }

    /// The summary line must state what was MEASURED — retry ATTEMPTS — and
    /// must never claim the retries recovered.
    ///
    /// [`RivetRetryNotify::intercept`] counts on the way INTO a retry (opendal's
    /// contract: fired "just before the retry sleep", inputs `(err, dur)`, no
    /// return channel; backon skips the notify entirely once the budget is
    /// exhausted), so the interceptor never learns whether the attempt it
    /// announced then succeeded — the last counted attempt can be the one that
    /// failed the run. RED against the pre-fix wording
    /// `"{n} transient (all recovered; RUST_LOG=debug for detail)"`, which made
    /// its strongest claim exactly on a run that died on a destination error.
    #[test]
    fn transient_retries_line_reports_attempts_and_never_claims_recovery() {
        use super::transient_retries_summary;
        assert_eq!(
            transient_retries_summary(0),
            None,
            "no row at all when the destination never retried"
        );
        // Two counts, not one: the line embeds the number, so a single sample
        // cannot tell a formatted count from a hard-coded one.
        for n in [1u64, 7] {
            let line = transient_retries_summary(n).expect("a row once retries > 0");
            assert!(
                line.contains(&format!("{n} transient retry attempts")),
                "line must report the attempt COUNT: {line:?}"
            );
            assert!(
                !line.to_ascii_lowercase().contains("recover"),
                "line must not claim recovery — the interceptor cannot observe \
                 the outcome of the attempt it counted: {line:?}"
            );
            assert!(
                line.contains("RUST_LOG=debug"),
                "line must still point at the per-attempt detail: {line:?}"
            );
        }
    }
}
