//! Test-only fault-injection hook for the export pipeline.
//!
//! The hook is deliberately implemented without a `#[cfg(test)]` gate or a
//! cargo feature — both would force a separate test-only build and require
//! the QA backlog Task 1.1 crash-point matrix to rebuild `rivet` with a
//! non-default flag.  Instead, we read the `RIVET_TEST_PANIC_AT` environment
//! variable **once** at startup (amortised to a single load on the first
//! call) and panic only if the current fault point matches.
//!
//! Runtime cost when the env var is unset: one relaxed atomic load per call
//! (measured: ~1 ns).  That is acceptable for the handful of
//! `maybe_panic_at` call-sites sprinkled through the write path.
//!
//! # Fault points
//!
//! | Point | Used in | Trigger format |
//! |---|---|---|
//! | `after_source_read` | `single.rs` | `RIVET_TEST_PANIC_AT=after_source_read` |
//! | `after_file_write` | `single.rs` | `RIVET_TEST_PANIC_AT=after_file_write` |
//! | `after_manifest_update` | `single.rs` | `RIVET_TEST_PANIC_AT=after_manifest_update` |
//! | `after_cursor_commit` | `single.rs` | `RIVET_TEST_PANIC_AT=after_cursor_commit` |
//! | `after_chunk_file:{N}` | `chunked/mod.rs` (sequential & parallel checkpoint) | `RIVET_TEST_PANIC_AT=after_chunk_file:0` |
//! | `after_chunk_complete:{N}` | `chunked/mod.rs` (sequential & parallel checkpoint) | `RIVET_TEST_PANIC_AT=after_chunk_complete:0` |
//! | `after_keyset_page:{N}` | `keyset.rs` | `RIVET_TEST_PANIC_AT=after_keyset_page:0` |
//! | `keyset_after_open_before_first_page` | `keyset.rs` (fresh run opened, no page committed — stale-cursor recovery guard) | `RIVET_TEST_PANIC_AT=keyset_after_open_before_first_page` |
//! | `keyset_after_data_complete` | `keyset.rs` (all pages committed, resume anchor cleared — post-data-failure guard) | `RIVET_TEST_PANIC_AT=keyset_after_data_complete` |
//!
//! # Test usage
//!
//! ```ignore
//! // cause rivet to panic between dest.write() and state.update()
//! let out = Command::new(RIVET_BIN)
//!     .env("RIVET_TEST_PANIC_AT", "after_file_write")
//!     .args(...)
//!     .output();
//!
//! // crash after chunk 0 is marked complete in the state DB
//! let out = Command::new(RIVET_BIN)
//!     .env("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")
//!     .args(...)
//!     .output();
//! ```

use std::sync::OnceLock;

fn configured_point() -> Option<&'static str> {
    static CELL: OnceLock<Option<String>> = OnceLock::new();
    CELL.get_or_init(|| std::env::var("RIVET_TEST_PANIC_AT").ok())
        .as_deref()
}

/// Panic with a clear message if the current fault point matches the one
/// configured via `RIVET_TEST_PANIC_AT`.  No-op otherwise.
///
/// This is the only test-hook primitive the pipeline uses; keeping it tiny
/// makes the call-sites easy to audit.
#[inline]
pub(crate) fn maybe_panic_at(point: &str) {
    if let Some(configured) = configured_point()
        && configured == point
    {
        panic!("rivet test-hook: injected crash at '{point}' (RIVET_TEST_PANIC_AT)");
    }
}

/// HARD-EXIT (`std::process::exit`) if the current fault point matches
/// `"{point}:{index}"`. Unlike `maybe_panic_at*`, this kills the process
/// IMMEDIATELY rather than unwinding — the only way to simulate a mid-run crash
/// from a WORKER thread inside `std::thread::scope`, where a panic would instead
/// be caught and deferred until the scope joins every other worker (by which
/// point they have all finished, defeating a "some ranges done, some not" crash).
/// Configure via `RIVET_TEST_PANIC_AT=keyset_parallel_range_committed:0`.
#[inline]
pub(crate) fn maybe_exit_at_index(point: &str, index: i64) {
    if let Some(configured) = configured_point()
        && *configured == format!("{point}:{index}")
    {
        eprintln!("rivet test-hook: injected HARD EXIT at '{point}:{index}' (RIVET_TEST_PANIC_AT)");
        std::process::exit(70);
    }
}

/// PAUSE for the configured milliseconds if `RIVET_TEST_PAUSE_AT` names this
/// point (`"{point}:{millis}"`). The fourth primitive beside panic / hard-exit /
/// returned-error: some tests need to construct a CONCURRENCY WINDOW — "a
/// writer commits AFTER the snapshot opens and BEFORE the read finishes" — and
/// without a pause the only tool is a sleep in the TEST racing rivet's own
/// startup. That race is exactly how the reconcile-mismatch fixture flaked both
/// ways (a 600 ms sleep lost to startup on a loaded box; a pg_stat_activity
/// wait turned a CI-green test red, cause never identified, reverted). A pause
/// at the product's own sequence point does not race anything: the window IS
/// open when the point is reached.
#[inline]
pub(crate) fn maybe_pause_at(point: &str) {
    if let Ok(p) = std::env::var("RIVET_TEST_PAUSE_AT")
        && let Some((configured, ms)) = p.rsplit_once(':')
        && configured == point
        && let Ok(ms) = ms.parse::<u64>()
    {
        eprintln!("rivet test-hook: pausing {ms}ms at '{point}' (RIVET_TEST_PAUSE_AT)");
        // Announce the window to the TEST before sleeping: the pause opens a
        // concurrency window, but the test's writer has no way to know it
        // opened — a writer on its own clock races rivet's startup, which is
        // the exact defect this hook exists to remove (measured: an immediate
        // writer landed its rows BEFORE the snapshot and the window was
        // pause-shaped but empty). Touching a file turns "the window is open"
        // into a pollable condition instead of a guess.
        if let Ok(marker) = std::env::var("RIVET_TEST_PAUSE_MARKER") {
            let _ = std::fs::write(&marker, point);
        }
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}

/// Return an `Err` if `RIVET_TEST_ERROR_AT` names exactly `point` — the
/// index-less sibling of [`maybe_error_at_index`], for fault points that are not
/// per-worker (a one-shot query whose TRANSIENT failure is the state a
/// no-swallowing contract exists for, and which a healthy stand can never
/// produce).
pub(crate) fn maybe_fail_at(point: &str) -> crate::error::Result<()> {
    if let Ok(p) = std::env::var("RIVET_TEST_ERROR_AT")
        && p == point
    {
        anyhow::bail!("rivet test-hook: injected error at '{point}' (RIVET_TEST_ERROR_AT)");
    }
    Ok(())
}

/// Return an `Err` if `RIVET_TEST_ERROR_AT` matches `"{point}:{index}"` — simulates a
/// per-worker SQL error (connection drop / statement timeout) MID-RANGE, distinct from
/// the hard-exit crash: the worker RETURNS an error (not process death), the parallel
/// runner collects it and bails, so NO `_SUCCESS` / manifest is finalized.
pub(crate) fn maybe_error_at_index(point: &str, index: i64) -> Result<(), String> {
    if let Ok(p) = std::env::var("RIVET_TEST_ERROR_AT")
        && p == format!("{point}:{index}")
    {
        return Err(format!(
            "rivet test-hook: injected error at '{point}:{index}' (RIVET_TEST_ERROR_AT)"
        ));
    }
    Ok(())
}

/// Panic if the current chunk-level fault point matches `"{point}:{chunk_index}"`.
///
/// Configure via `RIVET_TEST_PANIC_AT=after_chunk_complete:0` to crash after
/// chunk 0 is marked complete, or `after_chunk_file:0` to crash after the file
/// is written to the destination but before the chunk task is committed.
#[inline]
pub(crate) fn maybe_panic_at_chunk(point: &str, chunk_index: i64) {
    if let Some(configured) = configured_point()
        && *configured == format!("{point}:{chunk_index}")
    {
        panic!(
            "rivet test-hook: injected crash at '{point}' chunk {chunk_index} (RIVET_TEST_PANIC_AT)"
        );
    }
}

/// Block (sleep) at `point` for `RIVET_TEST_BLOCK_MS` ms (default 10000) when
/// `RIVET_TEST_BLOCK_AT` matches — a *deterministic* "process is alive mid-export"
/// window, distinct from the panic hook. Used to send a real signal to the parent
/// and assert subprocess children are reaped, not orphaned (OPT-6 crash matrix).
/// No-op (one memoised env read) when unset.
#[inline]
pub(crate) fn maybe_block_at(point: &str) {
    static BLOCK_POINT: OnceLock<Option<String>> = OnceLock::new();
    let configured = BLOCK_POINT
        .get_or_init(|| std::env::var("RIVET_TEST_BLOCK_AT").ok())
        .as_deref();
    if configured == Some(point) {
        let ms: u64 = std::env::var("RIVET_TEST_BLOCK_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000);
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}
