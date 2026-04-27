//! **Layer: Observability** — parent-side renderer for
//! `rivet run --parallel-export-processes`.
//!
//! Receives [`ipc::ChildEvent`]s from spawned children over an `mpsc` channel
//! and renders one *card* per export directly to stderr using a small ANSI
//! cursor protocol.  Each card is one header line, five per-field meta
//! lines, and one progress / final-summary line:
//!
//! ```text
//! ── orders ──────────────────────────────────────────────
//!   run_id:     orders_20260427T120000
//!   status:     running
//!   mode:       chunked
//!   tuning:     profile=balanced (default)
//!   batch_size: 10,000
//!   [====>--------------] 4/20 chunks | 100K rows | 00:00:09 | ETA 00:00:18
//! ```
//!
//! When the child emits `Finished`, the progress bar is replaced *in place*
//! with the final metrics so each card becomes a self-contained per-export
//! summary card:
//!
//! ```text
//! ── orders ──────────────────────────────────────────────
//!   run_id:     orders_20260427T120000
//!   status:     success
//!   mode:       chunked
//!   tuning:     profile=balanced (default)
//!   batch_size: 10,000
//!   rows: 100,000  files: 1  bytes: 1.2 MB  duration: 9.4s  peak RSS: 30 MB
//! ```
//!
//! ## Why a hand-rolled renderer (and not `indicatif`)?
//!
//! An earlier revision used `indicatif::MultiProgress` with one
//! `ProgressBar` per card line (header + 5 meta + progress).  In real ttys
//! the result was correct, but inside `vhs`/`ttyd` (the recording stack we
//! use for documentation GIFs) `indicatif` silently produced no output —
//! `console::Term::stderr().features().is_attended()` returned `true`, so
//! draws should have happened, but apparently no escape sequences ever
//! reached the captured frame.  The behaviour was reproducible across
//! several variants (`new(0)` vs `new_spinner()`, with/without
//! `enable_steady_tick`, `mp.println` vs `set_message`, etc.).
//!
//! Given that we already need to:
//!
//! - keep cards in a stable insertion order,
//! - keep them on screen permanently after the run (so they survive in
//!   scrollback),
//! - update progress / ETA on a fixed cadence even when no IPC event has
//!   arrived,
//!
//! the indicatif coordination buys us very little here.  This module owns
//! the screen for the duration of a `--parallel-export-processes` run,
//! redraws all cards on every event and on a 200 ms idle timer, and never
//! calls `indicatif`'s code paths.  That makes the rendering deterministic
//! across real terminals, recorded `vhs` sessions, and CI pipes (where the
//! ANSI sequences become harmless no-ops because no cursor exists to
//! reposition).

use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use super::format_bytes;
use super::ipc::ChildEvent;

/// One unit of work for the UI thread.  Children emit raw `ChildEvent`s; the
/// reader thread for each child wraps them in [`UiMessage::Event`].  When a
/// child's stdout closes (process exited) the reader sends [`UiMessage::ChildClosed`]
/// so the UI can mark a card whose `Finished` event was lost (e.g. crash).
#[derive(Debug)]
pub(crate) enum UiMessage {
    Event(ChildEvent),
    /// Child's stdout closed without a `Finished` event.  Carries the export
    /// name and an optional terminal verdict from `wait()`.
    ChildClosed {
        export_name: String,
        wait_status: ChildWaitStatus,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ChildWaitStatus {
    /// Used when we synthesize a `ChildClosed` from a worker that lost its
    /// child handle (we never managed to call `wait()`).  Currently only
    /// constructed by tests but documented here for completeness.
    #[allow(dead_code)]
    Pending,
    Success,
    Failed(String),
}

/// Width reserved for the `label:` column in meta lines so values line up
/// vertically (e.g. `batch_size:` is the longest label at 11 chars including
/// the colon).
const META_LABEL_WIDTH: usize = 11;

/// Width of the `=>--` progress bar inside the running card.
const PROGRESS_BAR_WIDTH: usize = 35;

/// How often the UI redraws when no event arrives (so elapsed-time and ETA
/// keep ticking on idle children).
const IDLE_REDRAW_INTERVAL: Duration = Duration::from_millis(200);

/// Drain `rx` until the channel closes, rendering events as multi-line
/// "cards" on stderr using a hand-rolled ANSI cursor protocol.
///
/// Width is recomputed once at startup from the terminal size and clamped to
/// `[60, 100]`.  Terminal resize during a run is not handled (rare, and
/// `indicatif` didn't handle it either inside our pty stack); the next run
/// picks up the new width.
pub(crate) fn run_ui(rx: Receiver<UiMessage>) {
    let width = pick_width();
    let mut renderer = Renderer::new(width);

    loop {
        match rx.recv_timeout(IDLE_REDRAW_INTERVAL) {
            Ok(msg) => renderer.process_message(msg),
            Err(RecvTimeoutError::Timeout) => {
                // No event in the last interval — redraw anyway so the
                // running cards' elapsed-time / ETA fields advance.
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
        renderer.redraw();
    }

    // Channel closed → all readers + producers gone.  Mark any cards still
    // running as failed (synthetic), do one last redraw at the same screen
    // position, then commit the cards to scrollback by emitting one
    // trailing newline (so the next stderr write — typically the aggregate
    // run summary — starts on a fresh line below the cards).
    for card in renderer.cards.values_mut() {
        if !card.finished {
            card.finalize_synthetic("failed", Some("child exited without a final summary"));
        }
    }
    renderer.redraw();
    renderer.commit();
}

/// Owns the on-screen state and knows how to repaint it.  `cards` is
/// addressed by export name; `order` is the insertion order in which we
/// observed each `Started` event so the card layout is deterministic across
/// runs even when the underlying `HashMap` iterates in a different order.
struct Renderer {
    cards: HashMap<String, CardState>,
    order: Vec<String>,
    /// Number of lines printed to stderr by the last `redraw()`.  Used so
    /// the next `redraw()` can move the cursor back to the same anchor and
    /// repaint in place rather than appending a fresh card stack each time.
    last_drawn_lines: usize,
    width: usize,
}

impl Renderer {
    fn new(width: usize) -> Self {
        Self {
            cards: HashMap::new(),
            order: Vec::new(),
            last_drawn_lines: 0,
            width,
        }
    }

    fn process_message(&mut self, msg: UiMessage) {
        match msg {
            UiMessage::Event(ev) => self.handle_event(ev),
            UiMessage::ChildClosed {
                export_name,
                wait_status,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name)
                    && !card.finished
                {
                    let (status, err) = match wait_status {
                        ChildWaitStatus::Success => ("success".to_string(), None),
                        ChildWaitStatus::Failed(msg) => ("failed".to_string(), Some(msg)),
                        ChildWaitStatus::Pending => (
                            "failed".to_string(),
                            Some("child exited without a final summary".into()),
                        ),
                    };
                    card.finalize_synthetic(&status, err.as_deref());
                }
            }
        }
    }

    fn handle_event(&mut self, ev: ChildEvent) {
        match ev {
            ChildEvent::Started {
                export_name,
                run_id,
                mode,
                tuning_profile,
                batch_size,
            } => {
                if !self.cards.contains_key(&export_name) {
                    self.order.push(export_name.clone());
                }
                let card = CardState {
                    export_name: export_name.clone(),
                    run_id,
                    mode,
                    tuning_profile,
                    batch_size,
                    status: "running".into(),
                    chunks_done: 0,
                    total_chunks: 0,
                    rows: 0,
                    started_at: Instant::now(),
                    finished: false,
                    final_line: String::new(),
                };
                self.cards.insert(export_name, card);
            }
            ChildEvent::ProgressInit {
                export_name,
                total_chunks,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.total_chunks = total_chunks;
                }
            }
            ChildEvent::Progress {
                export_name,
                chunks_done,
                rows,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.chunks_done = chunks_done;
                    card.rows = rows;
                }
            }
            ChildEvent::Finished {
                export_name,
                run_id,
                status,
                total_rows,
                files_produced,
                bytes_written,
                duration_ms,
                peak_rss_mb,
                error_message,
            } => {
                if let Some(card) = self.cards.get_mut(&export_name) {
                    card.finalize(
                        &run_id,
                        &status,
                        total_rows,
                        files_produced,
                        bytes_written,
                        duration_ms,
                        peak_rss_mb,
                        error_message.as_deref(),
                    );
                }
            }
        }
    }

    /// Repaint the card stack on stderr.  If a previous draw left
    /// `last_drawn_lines` lines on screen, move the cursor up that many
    /// lines first, then write each line, clearing it first so a shorter
    /// new line doesn't leave stale tail characters from the previous draw.
    fn redraw(&mut self) {
        let mut out = String::new();
        if self.last_drawn_lines > 0 {
            // CSI <n> A — move cursor up `n` lines (column unchanged).  We
            // assume the previous redraw left the cursor *below* the last
            // card line (after the trailing `\n`), which is the cursor
            // position the terminal naturally has after `writeln!`.
            out.push_str(&format!("\x1b[{}A", self.last_drawn_lines));
            // Move cursor to column 1 of that line.
            out.push('\r');
        }

        let mut new_lines = 0usize;
        for name in &self.order {
            if let Some(card) = self.cards.get(name) {
                for line in card.live_lines(self.width) {
                    // CSI 2K — erase entire line so a shorter new line
                    // overwrites the longer previous line cleanly.
                    out.push_str("\x1b[2K");
                    out.push_str(&line);
                    out.push('\n');
                    new_lines += 1;
                }
            }
        }

        // If the new layout is shorter than the previous one, blank out the
        // extra lines below so they don't linger from the last draw.
        if self.last_drawn_lines > new_lines {
            let extra = self.last_drawn_lines - new_lines;
            for _ in 0..extra {
                out.push_str("\x1b[2K\n");
            }
            // Move cursor back up so the next redraw's anchor is correct.
            out.push_str(&format!("\x1b[{extra}A"));
        }

        let mut handle = std::io::stderr().lock();
        let _ = handle.write_all(out.as_bytes());
        let _ = handle.flush();

        self.last_drawn_lines = new_lines;
    }

    /// Lock the rendered cards into scrollback.  After the last `redraw()`,
    /// cursor sits at the line *below* the last card (`writeln!` semantics).
    /// Future stderr writes (e.g. the aggregate run summary) will start
    /// there.  This is exactly what we want — no extra writing required.
    fn commit(&mut self) {
        // Reset to 0 so any later `redraw()` would print a fresh stack
        // beneath, but we don't expect any.
        self.last_drawn_lines = 0;
    }
}

/// Pure data for one export's card.  All formatting lives in `live_lines`.
struct CardState {
    export_name: String,
    run_id: String,
    mode: String,
    tuning_profile: String,
    batch_size: usize,
    status: String,
    chunks_done: u64,
    total_chunks: u64,
    rows: i64,
    started_at: Instant,
    finished: bool,
    final_line: String,
}

impl CardState {
    #[allow(clippy::too_many_arguments)]
    fn finalize(
        &mut self,
        run_id: &str,
        status: &str,
        total_rows: i64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: i64,
        peak_rss_mb: i64,
        error_message: Option<&str>,
    ) {
        self.finished = true;
        self.run_id = run_id.to_string();
        self.status = status.to_string();
        self.rows = total_rows;
        self.final_line = render_final_line(
            total_rows,
            files_produced,
            bytes_written,
            duration_ms,
            peak_rss_mb,
            error_message,
        );
    }

    fn finalize_synthetic(&mut self, status: &str, error_message: Option<&str>) {
        self.finished = true;
        self.status = status.to_string();
        self.final_line = format!(
            "  ⚠ {}",
            error_message.unwrap_or("child exited without a final summary")
        );
    }

    /// All seven lines the renderer should print for this card on the
    /// current frame.
    fn live_lines(&self, width: usize) -> Vec<String> {
        vec![
            format_header(width, &self.export_name),
            meta_line("run_id", &self.run_id),
            meta_line("status", &self.status),
            meta_line("mode", &self.mode),
            meta_line("tuning", &format!("profile={}", self.tuning_profile)),
            meta_line("batch_size", &fmt_thousands(self.batch_size as i64)),
            self.bottom_line(),
        ]
    }

    /// The 7th line: either the running progress bar (with ETA) or, once
    /// the card is finished, the final metrics / error message.
    fn bottom_line(&self) -> String {
        if self.finished {
            return self.final_line.clone();
        }
        let elapsed = self.started_at.elapsed();
        let elapsed_ms = elapsed.as_millis() as i64;
        let bar = render_progress_bar(self.chunks_done, self.total_chunks);
        let chunks_label = if self.total_chunks > 0 {
            format!("{}/{} chunks", self.chunks_done, self.total_chunks)
        } else {
            "preparing…".into()
        };
        let eta_label = if self.chunks_done > 0 && self.total_chunks > self.chunks_done {
            let total_ms_est =
                elapsed_ms as f64 * (self.total_chunks as f64 / self.chunks_done as f64);
            let remaining = (total_ms_est - elapsed_ms as f64).max(0.0) as i64;
            fmt_duration_ms(remaining)
        } else if self.total_chunks > 0 && self.chunks_done >= self.total_chunks {
            "0s".into()
        } else {
            "—".into()
        };
        format!(
            "  [{bar}] {chunks_label} | {rows} | {elapsed} | ETA {eta}",
            rows = fmt_rows(self.rows),
            elapsed = fmt_duration_ms(elapsed_ms),
            eta = eta_label,
        )
    }
}

/// Render one `  label:       value` line with a fixed-width label column so
/// values line up vertically across all meta lines in a card.
fn meta_line(label: &str, value: &str) -> String {
    let label_with_colon = format!("{label}:");
    let pad = META_LABEL_WIDTH.saturating_sub(label_with_colon.chars().count());
    format!("  {label_with_colon}{} {value}", " ".repeat(pad))
}

fn render_progress_bar(done: u64, total: u64) -> String {
    let mut s = String::with_capacity(PROGRESS_BAR_WIDTH);
    if total == 0 {
        return "-".repeat(PROGRESS_BAR_WIDTH);
    }
    let filled = ((done as f64 / total as f64) * PROGRESS_BAR_WIDTH as f64).floor() as usize;
    let filled = filled.min(PROGRESS_BAR_WIDTH);
    for i in 0..PROGRESS_BAR_WIDTH {
        let ch = if i < filled.saturating_sub(1) {
            '='
        } else if i == filled.saturating_sub(1) && done < total {
            '>'
        } else if i < filled {
            '='
        } else {
            '-'
        };
        s.push(ch);
    }
    s
}

fn render_final_line(
    total_rows: i64,
    files_produced: u64,
    bytes_written: u64,
    duration_ms: i64,
    peak_rss_mb: i64,
    error_message: Option<&str>,
) -> String {
    if let Some(err) = error_message {
        return format!("  ✗ {}", err);
    }
    let rss = if peak_rss_mb > 0 {
        format!("  peak RSS: {} MB", fmt_thousands(peak_rss_mb))
    } else {
        String::new()
    };
    format!(
        "  rows: {}  files: {}  bytes: {}  duration: {}{}",
        fmt_thousands(total_rows),
        fmt_thousands(files_produced as i64),
        format_bytes(bytes_written),
        fmt_duration_ms(duration_ms),
        rss
    )
}

fn format_header(width: usize, name: &str) -> String {
    // `── name ───────────…` clamped to `width`.
    let head = format!("── {} ", name);
    if head.chars().count() >= width {
        return head;
    }
    let pad = width - head.chars().count();
    format!("{}{}", head, "─".repeat(pad))
}

fn pick_width() -> usize {
    // `console::Term::stderr().size()` returns `(rows, cols)` if attached to a
    // tty, otherwise falls back to `(24, 80)` — that fallback is fine.
    let (_, cols) = console::Term::stderr().size();
    (cols as usize).clamp(60, 100)
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

fn fmt_thousands(n: i64) -> String {
    let abs = n.unsigned_abs();
    let raw = abs.to_string();
    let mut buf = String::with_capacity(raw.len() + raw.len() / 3);
    for (from_end, ch) in raw.chars().rev().enumerate() {
        if from_end > 0 && from_end.is_multiple_of(3) {
            buf.push(',');
        }
        buf.push(ch);
    }
    let s: String = buf.chars().rev().collect();
    if n < 0 { format!("-{s}") } else { s }
}

fn fmt_duration_ms(ms: i64) -> String {
    if ms < 0 {
        return "0s".into();
    }
    let total_secs_f = ms as f64 / 1000.0;
    if ms < 60_000 {
        return format!("{:.1}s", total_secs_f);
    }
    let mut secs = total_secs_f as i64;
    let hours = secs / 3600;
    secs %= 3600;
    let mins = secs / 60;
    let rem_secs = total_secs_f - (hours as f64 * 3600.0) - (mins as f64 * 60.0);
    if hours > 0 {
        format!("{}h {:02}m {:04.1}s", hours, mins, rem_secs)
    } else {
        format!("{}m {:04.1}s", mins, rem_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_padding_matches_width() {
        let h = format_header(60, "orders");
        assert!(h.starts_with("── orders "));
        // Visible width (counting box-drawing dashes as 1 column) is 60.
        assert_eq!(h.chars().count(), 60);
    }

    #[test]
    fn header_truncates_when_name_is_huge() {
        let h = format_header(20, "this_is_a_very_very_very_long_export_name");
        // Should not panic, should still start with `── ` and contain the
        // export name.  May exceed `width` (single-line truncation is
        // handled by the terminal, not by us).
        assert!(h.starts_with("── this_is_a_very_very_very_long_export_name "));
    }

    #[test]
    fn meta_line_pads_label_column() {
        let line = meta_line("run_id", "orders_20260427T120000");
        assert!(line.starts_with("  run_id:"));
        assert!(line.contains("orders_20260427T120000"));
        // Layout: `  ` (2) + `run_id:` (7) + 4 padding + ` ` (1 literal
        // space) + value.  Value's first character starts at index 14.
        let value_start = line.find('o').unwrap();
        assert_eq!(value_start, 14);
    }

    #[test]
    fn meta_lines_align_values_vertically() {
        let a = meta_line("run_id", "@@VAL@@");
        let b = meta_line("batch_size", "@@VAL@@");
        let pos_a = a.find("@@VAL@@").unwrap();
        let pos_b = b.find("@@VAL@@").unwrap();
        assert_eq!(pos_a, pos_b);
    }

    #[test]
    fn final_line_includes_metrics() {
        let s = render_final_line(123_456, 5, 1024 * 1024, 9_400, 30, None);
        assert!(s.contains("rows: 123,456"));
        assert!(s.contains("files: 5"));
        assert!(s.contains("duration: 9.4s"));
        assert!(s.contains("peak RSS: 30 MB"));
    }

    #[test]
    fn final_line_uses_error_message() {
        let s = render_final_line(0, 0, 0, 0, 0, Some("connection reset"));
        assert!(s.contains("connection reset"));
        assert!(s.starts_with("  ✗ "));
    }

    #[test]
    fn pick_width_in_bounds() {
        let w = pick_width();
        assert!((60..=100).contains(&w));
    }

    #[test]
    fn fmt_rows_buckets() {
        assert_eq!(fmt_rows(0), "0 rows");
        assert_eq!(fmt_rows(999), "999 rows");
        assert_eq!(fmt_rows(1_500), "2K rows");
        assert_eq!(fmt_rows(1_500_000), "1.5M rows");
    }

    #[test]
    fn fmt_thousands_basic() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(1234), "1,234");
        assert_eq!(fmt_thousands(-1234), "-1,234");
        assert_eq!(fmt_thousands(1_000_000), "1,000,000");
    }

    #[test]
    fn fmt_duration_buckets() {
        assert_eq!(fmt_duration_ms(0), "0.0s");
        assert_eq!(fmt_duration_ms(9_400), "9.4s");
        assert!(fmt_duration_ms(60_000).starts_with("1m"));
        assert!(fmt_duration_ms(3_600_000).starts_with("1h"));
    }

    #[test]
    fn render_progress_bar_endpoints() {
        let zero = render_progress_bar(0, 10);
        let full = render_progress_bar(10, 10);
        assert_eq!(zero.chars().count(), PROGRESS_BAR_WIDTH);
        assert_eq!(full.chars().count(), PROGRESS_BAR_WIDTH);
        assert!(zero.contains('-'));
        assert!(full.starts_with("====="));
    }

    #[test]
    fn render_progress_bar_midpoint_has_arrow() {
        let mid = render_progress_bar(5, 10);
        assert!(mid.contains('>'));
    }

    #[test]
    fn card_bottom_line_running_renders_chunks_or_preparing() {
        let mut card = CardState {
            export_name: "orders".into(),
            run_id: "rid".into(),
            mode: "chunked".into(),
            tuning_profile: "balanced (default)".into(),
            batch_size: 1_000,
            status: "running".into(),
            chunks_done: 0,
            total_chunks: 0,
            rows: 0,
            started_at: Instant::now(),
            finished: false,
            final_line: String::new(),
        };
        let line = card.bottom_line();
        assert!(line.contains("preparing"));

        card.total_chunks = 10;
        card.chunks_done = 4;
        card.rows = 40_000;
        let line = card.bottom_line();
        assert!(line.contains("4/10 chunks"));
        assert!(line.contains("40K rows"));
    }

    #[test]
    fn card_bottom_line_uses_final_when_finished() {
        let mut card = CardState {
            export_name: "orders".into(),
            run_id: "rid".into(),
            mode: "chunked".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1_000,
            status: "running".into(),
            chunks_done: 0,
            total_chunks: 0,
            rows: 0,
            started_at: Instant::now(),
            finished: false,
            final_line: String::new(),
        };
        card.finalize("rid_final", "success", 100, 1, 1024, 1234, 30, None);
        let line = card.bottom_line();
        assert!(line.contains("rows: 100"));
        assert!(line.contains("duration: 1.2s"));
    }

    #[test]
    fn card_finalize_synthetic_uses_warning_glyph() {
        let mut card = CardState {
            export_name: "orders".into(),
            run_id: "rid".into(),
            mode: "chunked".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1_000,
            status: "running".into(),
            chunks_done: 0,
            total_chunks: 0,
            rows: 0,
            started_at: Instant::now(),
            finished: false,
            final_line: String::new(),
        };
        card.finalize_synthetic("failed", Some("child crashed"));
        assert!(card.finished);
        assert_eq!(card.status, "failed");
        assert!(card.final_line.contains("⚠"));
        assert!(card.final_line.contains("child crashed"));
    }
}
