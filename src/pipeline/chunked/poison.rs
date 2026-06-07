//! Mutex poison recovery — one documented home for a decision the parallel
//! chunk drivers (`exec`, `parallel_checkpoint`) otherwise repeat at every lock.
//!
//! The chunk workers share a few `Mutex`-guarded accumulators: the governor
//! decision log, the `PartRecord` drain buffer, and the error list. If a worker
//! thread panics mid-chunk it poisons whatever lock it held — but the partial
//! state left behind is still valid to drain. The parent thread reads these
//! buffers *post-join* to finalize the manifest and surface errors, and a
//! panicked worker's already-pushed records are exactly what we want to keep
//! (dropping them would silently shrink the cloud manifest — the M1 contract
//! `parallel_checkpoint` exists to honour). So we deliberately recover from
//! poisoning rather than propagate it.
//!
//! These two helpers name that decision so it is stated once here instead of
//! being re-derived at each `.unwrap_or_else(|e| e.into_inner())` call site.

use std::sync::{Mutex, MutexGuard};

/// Lock a shared accumulator, recovering the guard if a panicked worker
/// poisoned it. See module docs for why poisoning is recovered, not propagated.
pub(super) fn lock_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Consume a shared accumulator post-join, recovering the inner value if a
/// panicked worker poisoned it. See module docs.
pub(super) fn into_recover<T>(m: Mutex<T>) -> T {
    m.into_inner().unwrap_or_else(|e| e.into_inner())
}
