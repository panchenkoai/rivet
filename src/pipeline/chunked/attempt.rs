//! One chunk's retry policy, shared by the two checkpoint runners.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::error::Result;
use crate::pipeline::RunSummary;
use crate::pipeline::retry::{Attempt, RetryClass, classify_error, retry_backoff_ms, should_retry};
use crate::plan::ResolvedRunPlan;

/// Retries and reconnects spent on chunks, shared across workers until folded into the summary.
#[derive(Default)]
pub(super) struct RetryTally {
    retries: AtomicU32,
    reconnects: AtomicU32,
}

impl RetryTally {
    /// Add the counts to the summary and zero them.
    pub(super) fn fold_into(&self, summary: &mut RunSummary) {
        summary.retries = summary
            .retries
            .saturating_add(self.retries.swap(0, Ordering::Relaxed));
        summary.reconnects = summary
            .reconnects
            .saturating_add(self.reconnects.swap(0, Ordering::Relaxed));
    }
}

/// Whether an attempt may reuse an idle connection: only the first; a retry reconnects.
pub(super) fn reuses_idle_connection(attempt: u32) -> bool {
    attempt == 0
}

/// A source for `attempt`: an idle connection on the first, a fresh one on a retry.
pub(super) fn open_for_attempt(
    idle: &super::IdleSources,
    cfg: &crate::config::SourceConfig,
    attempt: u32,
) -> Result<Box<dyn crate::source::Source>> {
    if reuses_idle_connection(attempt) {
        idle.take(cfg)
    } else {
        crate::source::create_source(cfg)
    }
}

/// Run one chunk with up to `max_retries` retries: `open` a source per attempt, `export` on it, hand the winning source to `keep`.
pub(super) fn run_with_retries<S, T>(
    plan: &ResolvedRunPlan,
    chunk_index: i64,
    tally: &RetryTally,
    mut open: impl FnMut(u32) -> Result<S>,
    mut export: impl FnMut(&mut S) -> Result<T>,
    keep: impl FnOnce(S),
) -> Result<T> {
    let max_retries = plan.tuning.max_retries;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=max_retries {
        if attempt > 0 {
            tally.retries.fetch_add(1, Ordering::Relaxed);
            let class = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or(RetryClass::Permanent);
            if class.needs_reconnect() {
                tally.reconnects.fetch_add(1, Ordering::Relaxed);
            }
            let backoff = retry_backoff_ms(
                plan.tuning.retry_backoff_ms,
                attempt,
                class.extra_delay_ms(),
            );
            log::warn!(
                "export '{}': chunk {} retry {}/{} in {}ms",
                plan.export_name,
                chunk_index,
                attempt,
                max_retries,
                backoff
            );
            std::thread::sleep(Duration::from_millis(backoff));
        }
        let retry = |error: &anyhow::Error| {
            should_retry(Attempt {
                attempt,
                max_retries,
                error,
            })
        };
        let mut src = match open(attempt) {
            Ok(s) => s,
            Err(e) if retry(&e) => {
                last_err = Some(e);
                continue;
            }
            Err(e) => {
                return Err(crate::pipeline::single::attach_connect_hint(
                    e,
                    &plan.source,
                ));
            }
        };
        match export(&mut src) {
            Ok(v) => {
                keep(src);
                return Ok(v);
            }
            Err(e) if retry(&e) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk export failed after retries")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    fn plan(max_retries: u32) -> ResolvedRunPlan {
        let mut p = crate::pipeline::commit::tests::test_plan();
        p.tuning.max_retries = max_retries;
        p.tuning.retry_backoff_ms = 0;
        p
    }

    /// Drives `run_with_retries` with an export that fails `fails` times with `err`; returns (result, opened attempts, kept source, tally).
    fn drive(
        max_retries: u32,
        fails: usize,
        err: &str,
    ) -> (Result<u32>, Vec<u32>, Option<u32>, RunSummary) {
        let tally = RetryTally::default();
        let opened = RefCell::new(Vec::new());
        let calls = Cell::new(0usize);
        let kept = Cell::new(None);
        let r = run_with_retries(
            &plan(max_retries),
            7,
            &tally,
            |a| {
                opened.borrow_mut().push(a);
                Ok(a)
            },
            |src: &mut u32| {
                calls.set(calls.get() + 1);
                if calls.get() <= fails {
                    return Err(anyhow::anyhow!("{err}"));
                }
                Ok(*src)
            },
            |s| kept.set(Some(s)),
        );
        let mut summary = RunSummary::default();
        tally.fold_into(&mut summary);
        (r, opened.into_inner(), kept.get(), summary)
    }

    #[test]
    fn a_chunk_retries_transient_failures_and_keeps_the_winning_source() {
        let (r, opened, kept, s) = drive(3, 2, "connection reset by peer");
        assert_eq!(r.unwrap(), 2, "the third attempt's source exported");
        assert_eq!(opened, vec![0, 1, 2], "one open per attempt");
        assert_eq!(kept, Some(2));
        assert_eq!(
            (s.retries, s.reconnects),
            (2, 2),
            "a reset is a reconnect-class retry"
        );
    }

    #[test]
    fn a_chunk_gives_up_after_max_retries_with_the_last_error() {
        let (r, opened, kept, s) = drive(2, 5, "connection reset by peer");
        assert!(r.unwrap_err().to_string().contains("connection reset"));
        assert_eq!(opened, vec![0, 1, 2]);
        assert_eq!(kept, None, "a failed chunk's connection is not kept");
        assert_eq!(s.retries, 2);
    }

    #[test]
    fn a_permanent_error_is_not_retried() {
        let (r, opened, _, s) = drive(3, 1, "syntax error at or near SELECT");
        assert!(r.is_err());
        assert_eq!(opened, vec![0]);
        assert_eq!((s.retries, s.reconnects), (0, 0));
    }

    #[test]
    fn a_same_connection_retry_is_not_counted_as_a_reconnect() {
        let (r, _, _, s) = drive(3, 1, "deadlock detected");
        assert!(r.is_ok());
        assert_eq!((s.retries, s.reconnects), (1, 0));
    }

    #[test]
    fn a_transient_open_failure_is_retried_and_a_final_one_returned() {
        let tally = RetryTally::default();
        let opens = Cell::new(0u32);
        let r: Result<u32> = run_with_retries(
            &plan(1),
            0,
            &tally,
            |_| {
                opens.set(opens.get() + 1);
                Err(anyhow::anyhow!("connection refused"))
            },
            |s: &mut u32| Ok(*s),
            |_| {},
        );
        assert!(r.is_err());
        assert_eq!(opens.get(), 2, "the open is retried within the budget");
    }

    #[test]
    fn only_the_first_attempt_reuses_an_idle_connection() {
        assert!(reuses_idle_connection(0));
        assert!(!reuses_idle_connection(1));
    }
}
