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
    if p.is_empty() || p.ends_with('/') {
        p
    } else {
        format!("{p}/")
    }
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
    use super::{AtomicI64, CloudDestination, Ordering, normalize_prefix, take_from};
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
