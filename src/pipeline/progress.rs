use std::sync::Arc;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

/// Chunk progress bar shown during chunked export runs.
///
/// Hidden when stderr is not a TTY (CI, redirected stderr).
/// Shared across threads via `Arc<ProgressBar>`.
pub(crate) struct ChunkProgress {
    bar: Arc<ProgressBar>,
}

impl ChunkProgress {
    pub(crate) fn new(export_name: &str, total_chunks: usize) -> Self {
        let bar = ProgressBar::new(total_chunks as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "  {prefix:.bold} [{bar:35.cyan/blue}] {pos}/{len} chunks | {msg} | {elapsed_precise} | ETA {eta}",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=>-"),
        );
        bar.set_prefix(export_name.to_string());
        bar.set_message("0 rows");
        bar.enable_steady_tick(Duration::from_millis(200));
        Self { bar: Arc::new(bar) }
    }

    /// Advance by one chunk and update the running row count.
    pub(crate) fn inc(&self, total_rows_so_far: i64) {
        self.bar.set_message(fmt_rows(total_rows_so_far));
        self.bar.inc(1);
    }

    /// Return a cloneable handle for worker threads.
    pub(crate) fn arc(&self) -> Arc<ProgressBar> {
        Arc::clone(&self.bar)
    }

    pub(crate) fn finish(&self, total_rows: i64) {
        self.bar
            .finish_with_message(format!("{} rows total", fmt_rows(total_rows)));
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
