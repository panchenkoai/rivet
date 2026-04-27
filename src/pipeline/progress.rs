//! **Layer: Observability**
//!
//! Terminal progress bar for chunked exports.
//!
//! Two display backends:
//! - **interactive** — when stderr is a TTY and `RIVET_IPC_EVENTS` is unset:
//!   draws an `indicatif::ProgressBar` on stderr.
//! - **IPC events** — when `RIVET_IPC_EVENTS=1` is set (we are a child of
//!   `rivet run --parallel-export-processes`): the visual bar is hidden and
//!   every advance emits a structured [`ipc::ChildEvent::Progress`] on stdout
//!   so the parent can draw a unified `MultiProgress` for all children.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use super::ipc::{self, ChildEvent};

/// Chunk progress bar shown during chunked export runs.
///
/// Hidden when stderr is not a TTY (CI, redirected stderr) or when this
/// process is a parallel-process child (events are emitted to the parent
/// instead).  Shared across worker threads via [`ChunkProgress::handle`].
pub(crate) struct ChunkProgress {
    bar: Arc<ProgressBar>,
    inner: Arc<HandleInner>,
}

/// Handle that worker threads hold.  Worker threads call [`Self::inc`] which
/// updates the underlying `indicatif` bar **and** emits an IPC event when
/// running as a child process, without each worker having to know about
/// `RIVET_IPC_EVENTS` itself.
#[derive(Clone)]
pub(crate) struct ChunkProgressHandle {
    bar: Arc<ProgressBar>,
    inner: Arc<HandleInner>,
}

struct HandleInner {
    export_name: String,
    /// Position counter used as the truth source for IPC `chunks_done`,
    /// because indicatif's internal counter is not directly readable in a
    /// thread-safe-yet-cheap way and we need a u64 we can ship in events.
    chunks_done: AtomicU64,
    ipc: bool,
}

impl ChunkProgress {
    pub(crate) fn new(export_name: &str, total_chunks: usize) -> Self {
        let ipc_on = ipc::ipc_events_enabled();
        let bar = if ipc_on {
            // Child mode: render nothing; the parent draws progress for us.
            ProgressBar::with_draw_target(Some(total_chunks as u64), ProgressDrawTarget::hidden())
        } else {
            let b = ProgressBar::new(total_chunks as u64);
            b.set_style(
                ProgressStyle::with_template(
                    "  {prefix:.bold} [{bar:35.cyan/blue}] {pos}/{len} chunks | {msg} | {elapsed_precise} | ETA {eta}",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=>-"),
            );
            b.set_prefix(export_name.to_string());
            b.set_message("0 rows");
            b.enable_steady_tick(Duration::from_millis(200));
            b
        };

        let bar = Arc::new(bar);
        let inner = Arc::new(HandleInner {
            export_name: export_name.to_string(),
            chunks_done: AtomicU64::new(0),
            ipc: ipc_on,
        });

        if ipc_on {
            ipc::emit(&ChildEvent::ProgressInit {
                export_name: export_name.to_string(),
                total_chunks: total_chunks as u64,
            });
        }

        Self { bar, inner }
    }

    /// Advance by one chunk and update the running row count (single-threaded
    /// caller).  Equivalent to `self.handle().inc(...)` but kept as a thin
    /// wrapper for the existing call sites that own a `ChunkProgress`
    /// directly.
    pub(crate) fn inc(&self, total_rows_so_far: i64) {
        self.handle_view().inc(total_rows_so_far);
    }

    /// Cloneable handle for worker threads.
    pub(crate) fn handle(&self) -> ChunkProgressHandle {
        ChunkProgressHandle {
            bar: Arc::clone(&self.bar),
            inner: Arc::clone(&self.inner),
        }
    }

    /// Internal handle without cloning the `Arc` ref — used by `Self::inc`.
    fn handle_view(&self) -> ChunkProgressHandle {
        ChunkProgressHandle {
            bar: Arc::clone(&self.bar),
            inner: Arc::clone(&self.inner),
        }
    }

    pub(crate) fn finish(&self, total_rows: i64) {
        // `finish_with_message` would leave the bar visible; in parallel runs
        // that lingering line gets interleaved into the next export's summary.
        // The final row count is duplicated in `RunSummary` (interactive) and
        // in the `Finished` IPC event (child mode), so clearing is safe.
        self.bar.set_message(fmt_rows(total_rows));
        self.bar.finish_and_clear();
    }
}

impl ChunkProgressHandle {
    /// Advance by one chunk.  Updates the local indicatif bar (a no-op in
    /// hidden mode) and emits a `Progress` IPC event when in child mode.
    pub(crate) fn inc(&self, total_rows_so_far: i64) {
        self.bar.set_message(fmt_rows(total_rows_so_far));
        self.bar.inc(1);
        let chunks_done = self.inner.chunks_done.fetch_add(1, Ordering::Relaxed) + 1;
        if self.inner.ipc {
            ipc::emit(&ChildEvent::Progress {
                export_name: self.inner.export_name.clone(),
                chunks_done,
                rows: total_rows_so_far,
            });
        }
    }

    /// Direct access to the underlying bar.  Some workers want to update the
    /// message without advancing — kept for parity with the previous
    /// `Arc<ProgressBar>` API.
    #[allow(dead_code)]
    pub(crate) fn set_message(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        self.bar.set_message(msg);
    }
}

fn fmt_rows(rows: i64) -> String {
    if rows >= 1_000_000 {
        format!("{:.1}M rows", rows as f64 / 1_000_000.0)
    } else if rows >= 1_000 {
        format!("{:.0}K rows", rows as f64 / 1_000.0)
    } else {
        format!("{rows} rows")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_inc_advances_counter() {
        let pb = ChunkProgress::new("orders", 10);
        let h = pb.handle();
        h.inc(100);
        h.inc(250);
        assert_eq!(pb.inner.chunks_done.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn fmt_rows_picks_unit() {
        assert_eq!(fmt_rows(0), "0 rows");
        assert_eq!(fmt_rows(500), "500 rows");
        assert_eq!(fmt_rows(1_500), "2K rows");
        assert_eq!(fmt_rows(2_500_000), "2.5M rows");
    }
}
