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
//! | `after_chunk_file:{N}` | `chunked/mod.rs` | `RIVET_TEST_PANIC_AT=after_chunk_file:0` |
//! | `after_chunk_complete:{N}` | `chunked/mod.rs` | `RIVET_TEST_PANIC_AT=after_chunk_complete:0` |
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
