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
//! Each call-site passes a stable string identifier (the "point name").
//! See the inline `maybe_panic_at` call sites in `src/pipeline/single.rs`
//! for the complete list.
//!
//! # Test usage
//!
//! ```ignore
//! // cause rivet to panic between dest.write() and state.update()
//! let out = Command::new(RIVET_BIN)
//!     .env("RIVET_TEST_PANIC_AT", "after_file_write")
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
