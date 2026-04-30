//! **Layer: Observability** — parent-side renderer for
//! `rivet run --parallel-export-processes`.
//!
//! Receives [`ipc::ChildEvent`]s from spawned children over an `mpsc` channel
//! and renders one *line* per export directly to stderr.  Per-run metadata
//! that is identical for every export (config path, tuning profile, batch
//! size, `run_id` formats) is intentionally not repeated per row — the run
//! aggregator at end-of-run already prints it once.
//!
//! Running:
//! ```text
//! ▸ orders            chunked   3/20 chunks   300K rows   1m 53.8s  ETA 10m 44.9s
//! ```
//!
//! Finished (success):
//! ```text
//! ✓ orders            chunked   1,000,908 rows  11 files  32.4 MB  1m 44.4s  RSS 50 MB
//! ```
//!
//! Finished (failed):
//! ```text
//! ✗ metric_samples    chunked   chunk checkpoint run 'metric_samples_2026…' still in progress
//! ```
//!
//! Recovery commands for failed exports are NOT inlined here (they would
//! wrap and corrupt the cursor-up math).  The run aggregator prints one
//! consolidated `recovery:` block at the end instead.
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

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use super::ipc::ChildEvent;
use super::{clamp_line, format_bytes, strip_chunked_recovery_hint};

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

/// How often the UI redraws when no event arrives (so elapsed-time and ETA
/// keep ticking on idle children).
const IDLE_REDRAW_INTERVAL: Duration = Duration::from_millis(200);

/// Minimum padding for the name column so names stay aligned even when only
/// short ones have been observed so far.
const MIN_NAME_COL: usize = 12;

/// Minimum padding for the mode column.  Modes are short (`full`, `chunked`,
/// `incremental`, `timewindow`) so a fixed minimum is fine.
const MIN_MODE_COL: usize = 7;

/// Drain `rx` until the channel closes, rendering events as multi-line
/// "cards" on stderr.
///
/// Width is recomputed once at startup from the terminal size and clamped to
/// `[60, 100]`.  Terminal resize during a run is not handled (rare, and
/// `indicatif` didn't handle it either inside our pty stack); the next run
/// picks up the new width.
///
/// Two rendering modes:
///
/// - **Interactive (stderr is a tty)**: hand-rolled ANSI cursor protocol
///   that repaints all cards in place every 200 ms, so progress / ETA tick
///   forward and finished cards swap their progress bar for a final-metrics
///   line without scrolling the screen.
/// - **Linear (stderr is piped/redirected)**: cursor-up sequences would be
///   no-ops or, worse, line-wrap around long error messages and corrupt the
///   anchor — so we instead print each card exactly once when it
///   transitions to its terminal state.
pub(crate) fn run_ui(rx: Receiver<UiMessage>) {
    let width = pick_width();
    if console::Term::stderr().features().is_attended() {
        run_ui_interactive(rx, width);
    } else {
        run_ui_linear(rx, width);
    }
}

/// In-place card-stack renderer for tty stderr.
fn run_ui_interactive(rx: Receiver<UiMessage>, width: usize) {
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

/// Linear renderer for non-tty stderr.  Each card is printed once when it
/// reaches its terminal state — there is no in-place redraw, so output is
/// safe to `tee` / capture without phantom duplicates.
fn run_ui_linear(rx: Receiver<UiMessage>, width: usize) {
    let mut renderer = Renderer::new(width);
    let mut printed: HashSet<String> = HashSet::new();

    while let Ok(msg) = rx.recv() {
        renderer.process_message(msg);
        flush_finished_cards(&renderer, &mut printed, width);
    }

    // Channel closed: synthesize failures for anything still pending and
    // emit them too.
    let pending: Vec<String> = renderer
        .order
        .iter()
        .filter(|n| !printed.contains(n.as_str()))
        .cloned()
        .collect();
    for name in &pending {
        if let Some(card) = renderer.cards.get_mut(name)
            && !card.finished
        {
            card.finalize_synthetic("failed", Some("child exited without a final summary"));
        }
    }
    flush_finished_cards(&renderer, &mut printed, width);
}

/// Append every card that has reached its terminal state but hasn't been
/// printed yet to stderr, preserving the `Started`-event observation order.
fn flush_finished_cards(renderer: &Renderer, printed: &mut HashSet<String>, width: usize) {
    let (name_col, mode_col) = column_widths(&renderer.cards);
    let mut out = String::new();
    for name in &renderer.order {
        if printed.contains(name) {
            continue;
        }
        let Some(card) = renderer.cards.get(name) else {
            continue;
        };
        if !card.finished {
            continue;
        }
        for line in card.live_lines(width, name_col, mode_col) {
            out.push_str(&line);
            out.push('\n');
        }
        printed.insert(name.clone());
    }
    if out.is_empty() {
        return;
    }
    let mut handle = std::io::stderr().lock();
    let _ = handle.write_all(out.as_bytes());
    let _ = handle.flush();
}

/// Pick stable column widths for the name and mode columns by combining the
/// observed maxima with `MIN_NAME_COL` / `MIN_MODE_COL` floors so the layout
/// stays aligned even when only short names / modes have been observed yet.
fn column_widths(cards: &HashMap<String, CardState>) -> (usize, usize) {
    let mut name = 0usize;
    let mut mode = 0usize;
    for c in cards.values() {
        name = name.max(c.export_name.chars().count());
        mode = mode.max(c.mode.chars().count());
    }
    (name.max(MIN_NAME_COL), mode.max(MIN_MODE_COL))
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
                run_id: _,
                mode,
                tuning_profile: _,
                batch_size: _,
            } => {
                if !self.cards.contains_key(&export_name) {
                    self.order.push(export_name.clone());
                }
                let card = CardState {
                    export_name: export_name.clone(),
                    mode,
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
                run_id: _,
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

        let (name_col, mode_col) = column_widths(&self.cards);
        let mut new_lines = 0usize;
        for name in &self.order {
            if let Some(card) = self.cards.get(name) {
                for line in card.live_lines(self.width, name_col, mode_col) {
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
    mode: String,
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
        status: &str,
        total_rows: i64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: i64,
        peak_rss_mb: i64,
        error_message: Option<&str>,
    ) {
        self.finished = true;
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
        self.status = format!("synthetic-{}", status);
        self.final_line = error_message
            .unwrap_or("child exited without a final summary")
            .to_string();
    }

    /// Lines this card contributes to the current frame.  Compact mode
    /// emits exactly one line per export so 15 parallel exports take 15
    /// stderr rows instead of 105.  The line is clamped to `width`
    /// characters: a wrapped line would corrupt the cursor-up math in the
    /// in-place renderer and cause cards to drift down the screen across
    /// redraws.
    fn live_lines(&self, width: usize, name_col: usize, mode_col: usize) -> Vec<String> {
        vec![clamp_line(&self.compact_line(name_col, mode_col), width)]
    }

    /// Render the single per-export status line.  Layout:
    /// `<icon> <name pad> <mode pad>  <body>`
    fn compact_line(&self, name_col: usize, mode_col: usize) -> String {
        let icon = self.status_icon();
        let body = if self.finished {
            self.final_line.clone()
        } else {
            self.running_body()
        };
        format!(
            "{} {:<name$}  {:<mode$}  {}",
            icon,
            self.export_name,
            self.mode,
            body,
            name = name_col,
            mode = mode_col,
        )
    }

    fn status_icon(&self) -> &'static str {
        if !self.finished {
            return "▸";
        }
        match self.status.as_str() {
            "success" => "✓",
            "failed" => "✗",
            // `finalize_synthetic` rewrites status to `synthetic-<status>`
            // so we can distinguish a real failure (where the child sent a
            // `Finished` event) from a child whose stdout closed without
            // one.  Both still mean "did not succeed", but the glyph
            // hints at the difference for triage.
            s if s.starts_with("synthetic-") => "⚠",
            _ => "•",
        }
    }

    /// Body used while the export is still running.  Shape depends on
    /// whether chunk progress is known yet.
    fn running_body(&self) -> String {
        let elapsed_ms = self.started_at.elapsed().as_millis() as i64;
        let chunks_label = if self.total_chunks > 0 {
            format!("{}/{} chunks", self.chunks_done, self.total_chunks)
        } else {
            "preparing…".to_string()
        };
        let eta_label = if self.chunks_done > 0 && self.total_chunks > self.chunks_done {
            let total_ms_est =
                elapsed_ms as f64 * (self.total_chunks as f64 / self.chunks_done as f64);
            let remaining = (total_ms_est - elapsed_ms as f64).max(0.0) as i64;
            fmt_duration_ms(remaining)
        } else if self.total_chunks > 0 && self.chunks_done >= self.total_chunks {
            "0s".to_string()
        } else {
            "—".to_string()
        };
        format!(
            "{chunks}   {rows}   {elapsed}  ETA {eta}",
            chunks = chunks_label,
            rows = fmt_rows(self.rows),
            elapsed = fmt_duration_ms(elapsed_ms),
            eta = eta_label,
        )
    }
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
        // Drop the inline `rivet …` recovery hint — the run aggregator
        // prints one consolidated recovery block at the end, and the
        // long inline command would otherwise wrap and corrupt the
        // in-place card layout.
        let (cause, _) = strip_chunked_recovery_hint(err);
        return cause.to_string();
    }
    let rss = if peak_rss_mb > 0 {
        format!("  RSS {} MB", fmt_thousands(peak_rss_mb))
    } else {
        String::new()
    };
    format!(
        "{} rows  {} files  {}  {}{}",
        fmt_thousands(total_rows),
        fmt_thousands(files_produced as i64),
        format_bytes(bytes_written),
        fmt_duration_ms(duration_ms),
        rss
    )
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

    fn fresh_card(name: &str, mode: &str) -> CardState {
        CardState {
            export_name: name.into(),
            mode: mode.into(),
            status: "running".into(),
            chunks_done: 0,
            total_chunks: 0,
            rows: 0,
            started_at: Instant::now(),
            finished: false,
            final_line: String::new(),
        }
    }

    #[test]
    fn final_line_includes_metrics() {
        let s = render_final_line(123_456, 5, 1024 * 1024, 9_400, 30, None);
        assert!(s.contains("123,456 rows"));
        assert!(s.contains("5 files"));
        assert!(s.contains("9.4s"));
        assert!(s.contains("RSS 30 MB"));
    }

    #[test]
    fn final_line_uses_error_message() {
        let s = render_final_line(0, 0, 0, 0, 0, Some("connection reset"));
        assert_eq!(s, "connection reset");
    }

    #[test]
    fn final_line_strips_chunked_recovery_hint() {
        let s = render_final_line(
            0,
            0,
            0,
            0,
            0,
            Some(
                "export 'x': chunk checkpoint run 'rid' still in progress; \
                  use `rivet run --config foo.yaml --export x --resume` or \
                  `rivet state reset-chunks --config foo.yaml --export x`",
            ),
        );
        assert_eq!(
            s,
            "export 'x': chunk checkpoint run 'rid' still in progress"
        );
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
    fn compact_running_card_shows_progress_or_preparing() {
        let mut card = fresh_card("orders", "chunked");
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("▸ "), "running uses ▸: {}", line);
        assert!(line.contains("orders"));
        assert!(line.contains("chunked"));
        assert!(line.contains("preparing"));

        card.total_chunks = 10;
        card.chunks_done = 4;
        card.rows = 40_000;
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.contains("4/10 chunks"));
        assert!(line.contains("40K rows"));
        assert!(line.contains("ETA "));
    }

    #[test]
    fn compact_finished_card_shows_metrics_with_check() {
        let mut card = fresh_card("orders", "chunked");
        card.finalize("success", 100, 1, 1024, 1234, 30, None);
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("✓ "), "success uses ✓: {}", line);
        assert!(line.contains("100 rows"));
        assert!(line.contains("1 files"));
        assert!(line.contains("1.2s"));
        assert!(line.contains("RSS 30 MB"));
    }

    #[test]
    fn compact_failed_card_shows_cause_only() {
        let mut card = fresh_card("metric_samples", "chunked");
        card.finalize(
            "failed",
            0,
            0,
            0,
            0,
            0,
            Some(
                "export 'metric_samples': chunk checkpoint run 'rid' still in progress; \
                  use `rivet run --config foo.yaml --export metric_samples --resume` or \
                  `rivet state reset-chunks --config foo.yaml --export metric_samples`",
            ),
        );
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(line.starts_with("✗ "), "failed uses ✗: {}", line);
        assert!(line.contains("still in progress"));
        // The recovery hint must NOT survive into the per-card line.
        assert!(!line.contains("`rivet "), "hint must be stripped: {}", line);
    }

    #[test]
    fn compact_synthetic_failure_uses_warning_glyph() {
        let mut card = fresh_card("orders", "chunked");
        card.finalize_synthetic("failed", Some("child crashed"));
        let line = card.compact_line(MIN_NAME_COL, MIN_MODE_COL);
        assert!(card.finished);
        assert!(line.starts_with("⚠ "), "synthetic uses ⚠: {}", line);
        assert!(line.contains("child crashed"));
    }

    #[test]
    fn compact_line_pads_columns_for_alignment() {
        let mut a = fresh_card("a", "full");
        a.finalize("success", 1, 1, 0, 1000, 0, None);
        let mut b = fresh_card("longer_name", "incremental");
        b.finalize("success", 1, 1, 0, 1000, 0, None);
        let name_col = "longer_name".chars().count();
        let mode_col = "incremental".chars().count();
        let la = a.compact_line(name_col, mode_col);
        let lb = b.compact_line(name_col, mode_col);
        // The body separator (`{N} rows`) should start at the same column
        // for both since both name and mode are padded.
        let pa = la.find(" rows").expect("body present in a");
        let pb = lb.find(" rows").expect("body present in b");
        assert_eq!(pa, pb, "alignment broken: a={la:?} b={lb:?}");
    }
}
