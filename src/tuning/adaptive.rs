//! Adaptive batch sizing — live-feedback loop that reacts to DB pressure.
//!
//! Both `PostgresSource` and `MysqlSource` sample pressure metrics every
//! [`ADAPTIVE_SAMPLE_INTERVAL`] batches (`pg_stat_bgwriter.checkpoints_req` for
//! PG; `Innodb_log_waits` for MySQL) and call [`next_adaptive_batch_size`] to
//! pick the next fetch size.
//!
//! The OPT-2 [`Governor`] runs the same idea at the *parallelism* layer: every
//! [`GOVERNOR_SAMPLE_INTERVAL_MS`] it samples a [`PressureSource`], folds the
//! reading through [`GovernorState`], and emits each `(from, to)` transition
//! through a callback. Extracted from an inline `thread::scope` closure so the
//! decision loop is unit-testable on a fake `PressureSource` without
//! requiring a live database + a multi-second wait for two real sample
//! intervals. The runner binds the callback to its own semaphore-resize +
//! log + decision-log machinery.

use std::time::{Duration, Instant};

/// Number of batches between adaptive pressure samples.
pub const ADAPTIVE_SAMPLE_INTERVAL: usize = 10;

/// Hard floor for the adaptive fetch size — the loop never shrinks below this.
pub const ADAPTIVE_MIN_BATCH: usize = 500;

/// Decide the next adaptive fetch size from current pressure state.
///
/// - Under pressure: shrink to 75 %, but never below [`ADAPTIVE_MIN_BATCH`].
/// - Otherwise: grow to 125 %, but never above the schema-chosen `base` ceiling
///   (so we recover toward the initial fetch size without overshooting it).
///
/// Pure function — exported so adaptive batch-sizing can be unit-tested without
/// a live database.
pub fn next_adaptive_batch_size(current: usize, base: usize, under_pressure: bool) -> usize {
    if under_pressure {
        (current * 3 / 4).max(ADAPTIVE_MIN_BATCH)
    } else {
        (current * 5 / 4).min(base)
    }
}

/// Milliseconds between governor pressure samples (the parallelism control
/// loop in `pipeline::chunked::exec`). Coarser than batch-size adaptation:
/// spinning workers up/down churns connections, so the governor reacts more
/// deliberately than the per-batch fetch-size loop.
pub const GOVERNOR_SAMPLE_INTERVAL_MS: u64 = 1500;

/// Decide the next worker/connection count from current governor state.
///
/// Steps by **one** toward the bounds — gentler than the batch loop's ±25 %
/// ratio, because permit counts are small integers and each step opens or
/// retires a real source connection:
/// - Under pressure: shed one worker, never below `min`.
/// - Otherwise: recover one worker, never above `max`.
///
/// `min` is floored at 1 (a 0 ceiling would stall the pool) and `max` at `min`.
/// Pure function — exported so the governor can be unit-tested without a live DB.
///
/// The step is deliberately SYMMETRIC (one down, one up), which — combined with
/// [`GovernorState::observe`]'s strictly-rising pressure test — means a shed
/// holds only while the signal keeps rising sample over sample. See that
/// method's docs for why quick recovery is the intended policy and what it
/// costs on a coarse per-event counter.
pub fn next_parallel(current: usize, min: usize, max: usize, under_pressure: bool) -> usize {
    let lo = min.max(1);
    let hi = max.max(lo);
    let cur = current.clamp(lo, hi);
    if under_pressure {
        cur.saturating_sub(1).max(lo)
    } else {
        (cur + 1).min(hi)
    }
}

/// Consecutive unreadable samples (`None`) before the governor declares its
/// pressure signal LOST: warns once and starts stepping back toward the ceiling.
///
/// A single miss is a transient (a statement timeout, a busy catalog view) and
/// must not move parallelism, so this is >1. It must also be small enough that
/// the frozen window is bounded: at the production
/// [`GOVERNOR_SAMPLE_INTERVAL_MS`] three misses is ~4.5 s, against runs that are
/// hours long.
pub const GOVERNOR_BLIND_SAMPLES: usize = 3;

/// What a blind span (≥ [`GOVERNOR_BLIND_SAMPLES`] consecutive unreadable
/// samples) actually MEANS — the distinction the runner must not conflate,
/// because the two states call for opposite operator responses.
///
/// [`Lost`](Self::Lost) is an incident: the signal WAS read on this run and
/// stopped (monitor connection reaped by a pooler, server restart, tunnel
/// drop), so back-off protection degraded mid-flight and the operator may want
/// to look at the connection. [`NeverReadable`](Self::NeverReadable) is a
/// capability gap, permanent and already reported ONCE by the harness's
/// up-front arm probe — the engine has no foreign-pressure counter, or the
/// login cannot read it (MSSQL without `VIEW SERVER STATE`, a MySQL build
/// without `Innodb_log_waits`, MongoDB which never overrides the
/// `Source::sample_governor_pressure` default). Reporting the second as the
/// first told the operator a healthy monitoring connection had died — twice per
/// export, contradicting the probe line printed ~4.5 s earlier, and drowning
/// the line that means something real (bughunt 2026-08-14).
///
/// Both fail OPEN — the step back toward the ceiling is identical; only the
/// REPORT differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlindSignal {
    /// A readable signal stopped being readable. Worth a `warn`.
    Lost,
    /// The signal was never readable on this run — the arm probe already said
    /// so; the loop must not say it again in the language of a loss.
    NeverReadable,
}

/// Why the governor moved parallelism. Carried to the runner so the log line
/// and the journalled `ParallelismAdjusted` reason state what actually
/// happened: a step taken while BLIND is a fail-open, not evidence that
/// "source pressure eased".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionCause {
    /// The sample rose over the previous one: shed a worker.
    PressureRising,
    /// The sample was readable and flat/falling: hand a worker back.
    PressureEased,
    /// The sample could not be read at all and the blind span has passed
    /// [`GOVERNOR_BLIND_SAMPLES`]: step back toward the ceiling because an
    /// unreadable signal is not evidence of pressure.
    SignalUnreadable,
}

/// Decision state for the concurrency governor's control loop.
///
/// Holds the previous pressure sample and the current target parallelism so the
/// I/O parts (pressure sampling, semaphore resize, journaling) stay in the
/// execution layer while this — the actual policy — is unit-testable without a
/// live database or threads. `under_pressure` mirrors the batch loop: a sample
/// strictly higher than the previous one means pressure is rising.
#[derive(Debug)]
pub struct GovernorState {
    prev: Option<u64>,
    current: usize,
    floor: usize,
    ceiling: usize,
    /// Consecutive `None` samples since the last readable one.
    blind: usize,
    /// Has the pressure signal EVER been read on this run — by a loop sample or
    /// by the harness's arm probe ([`note_arm_probe`](Self::note_arm_probe))?
    /// This is what separates [`BlindSignal::Lost`] from
    /// [`BlindSignal::NeverReadable`]; `prev.is_some()` is NOT a substitute,
    /// because the arm probe's reading is deliberately not fed to the baseline.
    ever_readable: bool,
    /// One-shot: set on the tick where `blind` first reaches
    /// [`GOVERNOR_BLIND_SAMPLES`], drained by [`GovernorState::take_blind_report`]
    /// so the runner reports ONCE per blindness episode, not once per tick.
    blind_report: Option<BlindSignal>,
}

impl GovernorState {
    /// Start at `start`, clamped into `[floor, ceiling]`. `ceiling` is floored
    /// at 1 and `floor` clamped into `[1, ceiling]`.
    pub fn new(start: usize, floor: usize, ceiling: usize) -> Self {
        let ceiling = ceiling.max(1);
        let floor = floor.clamp(1, ceiling);
        Self {
            prev: None,
            current: start.clamp(floor, ceiling),
            floor,
            ceiling,
            blind: 0,
            ever_readable: false,
            blind_report: None,
        }
    }

    /// Record the outcome of the harness's UP-FRONT arm probe: `true` when that
    /// probe read the pressure signal.
    ///
    /// Only the readability crosses this seam, never the value — the baseline
    /// still comes from the loop's own first sample (see the runner's
    /// `GovernorHarness::spawn_into`). Without it a monitor connection
    /// that answered the probe and died before sample 1 would leave `prev ==
    /// None` and be misreported as [`BlindSignal::NeverReadable`] — a real loss
    /// silenced.
    pub fn note_arm_probe(&mut self, readable: bool) {
        self.ever_readable |= readable;
    }

    /// Current target parallelism. Test-only observability accessor.
    #[cfg(test)]
    pub fn current(&self) -> usize {
        self.current
    }

    /// Is the loop currently BLIND — i.e. has the run of unreadable samples
    /// reached [`GOVERNOR_BLIND_SAMPLES`], so this tick's step is a fail-open
    /// rather than a pressure reading? Drives [`DecisionCause`].
    pub fn is_blind(&self) -> bool {
        self.blind >= GOVERNOR_BLIND_SAMPLES
    }

    /// Fold one pressure sample into the state. Returns `Some((from, to))` when
    /// the target changed (caller should resize the semaphore + journal it), or
    /// `None` when nothing changed.
    ///
    /// Two policies live here, both learned the expensive way:
    ///
    /// **Rising-only pressure, immediate recovery.** `under_pressure` is
    /// `cur > prev` and *anything else* (flat, falling) recovers a worker, so a
    /// shed persists only while the signal keeps rising on EVERY sample. The
    /// consequence is honest and deliberate: on a COARSE counter — PostgreSQL's
    /// governor signal counts REQUESTED checkpoints only (`checkpoints_req`, or
    /// `pg_stat_checkpointer.num_requested` on PG 17+), which ticks only when WAL
    /// volume forces a checkpoint, i.e. far apart relative to the 1500 ms sample
    /// interval (PostgreSQL flags requested checkpoints closer together than
    /// `checkpoint_warning`, default 30 s, as a misconfiguration) — one rise
    /// sheds a worker and the next flat sample puts it straight back, so
    /// sustained foreign write pressure yields a repeated one-worker dip rather
    /// than a held-down level. Hysteresis (hold the shed for K samples) would deepen
    /// the back-off, and is NOT what this loop wants: the regression this
    /// governor was rewritten for (#229) was a shed that would not come back —
    /// it read its own extraction as foreign pressure and walked 4→3→2→1, and a
    /// field pool run lost 1h48m to it. A shed that is cheap to undo bounds the
    /// blast radius of a mis-read signal; a shed that is sticky does not. The
    /// deeper fix for a coarse engine is a finer-grained SIGNAL (a monotonically
    /// rising write counter), not a stickier policy on top of a signal that
    /// barely moves — changing the policy would make every engine's false
    /// positive more expensive to buy protection on one.
    ///
    /// **Fail open when the signal cannot be read.** A `None` sample holds
    /// parallelism flat and preserves the baseline — right for a transient miss.
    /// But nothing bounded a PERMANENT one: a monitoring connection reaped
    /// mid-run (pooler idle timeout, server restart, tunnel drop) makes every
    /// later sample `None`, and the run then stayed pinned at whatever level the
    /// last shed reached for the rest of its hours, silently. After
    /// [`GOVERNOR_BLIND_SAMPLES`] consecutive misses the state files a
    /// [`take_blind_report`](Self::take_blind_report) (the runner reports once,
    /// and — [`BlindSignal`] — only calls it a LOSS when the signal was ever
    /// readable) and treats the tick as NOT under pressure, stepping back toward the ceiling —
    /// a signal that cannot be read is not evidence of pressure, and this repo's
    /// convention on an unreadable boundary is to fail open (a run that is too
    /// fast for the source sheds again the moment the signal returns; a run
    /// silently pinned at the floor recovers never).
    pub fn observe(&mut self, sample: Option<u64>) -> Option<(usize, usize)> {
        let under_pressure = match sample {
            Some(cur_p) => {
                self.blind = 0;
                self.ever_readable = true;
                let rising = self.prev.is_some_and(|p| cur_p > p);
                self.prev = Some(cur_p);
                rising
            }
            None => {
                self.blind += 1;
                if self.blind < GOVERNOR_BLIND_SAMPLES {
                    // Transient miss: hold flat, keep the baseline so a later
                    // reading is still compared against the last real one.
                    return None;
                }
                if self.blind == GOVERNOR_BLIND_SAMPLES {
                    // WHICH blind span this is decides what the operator is
                    // told; the fail-open below is the same either way.
                    self.blind_report = Some(if self.ever_readable {
                        BlindSignal::Lost
                    } else {
                        BlindSignal::NeverReadable
                    });
                }
                false
            }
        };
        let next = next_parallel(self.current, self.floor, self.ceiling, under_pressure);
        if next == self.current {
            None
        } else {
            let from = self.current;
            self.current = next;
            Some((from, next))
        }
    }

    /// Drain the one-shot blind-span report (see [`observe`](Self::observe)).
    /// `Some` at most once per blindness episode — the flag re-arms only after
    /// a readable sample resets `blind`, so a permanently dead monitor
    /// connection produces ONE line, not one per sample interval for the rest
    /// of the run. A signal that was NEVER readable can therefore report
    /// [`BlindSignal::NeverReadable`] at most once, ever, on a run.
    pub fn take_blind_report(&mut self) -> Option<BlindSignal> {
        self.blind_report.take()
    }
}

/// How often the governor's `run` loop wakes to check the stop condition.
/// Kept much shorter than [`GOVERNOR_SAMPLE_INTERVAL_MS`] so the thread exits
/// promptly when the run finishes, instead of lingering for a full sample
/// interval after the last chunk completes.
pub const GOVERNOR_POLL_MS: u64 = 200;

/// Narrow seam the [`Governor`] needs from a source: hand it one pressure
/// reading. Implemented for `Box<dyn crate::source::Source>` so the
/// production runner can pass its already-built monitor connection in
/// directly; tests pass a small in-memory adapter (see `VecSource` in
/// this module's `tests`) so the decision loop is exercised without
/// touching a live database.
///
/// `Send` because the runner spawns the governor on its own thread
/// inside `thread::scope`.
pub trait PressureSource: Send {
    /// Return the source's current pressure reading, or `None` when the
    /// source cannot sample this tick (the governor then holds parallelism
    /// flat — see [`GovernorState::observe`]).
    /// One reading of the governor's foreign-pressure signal. (Named `sample`,
    /// not `sample_pressure`: the latter was the retired Source-trait method
    /// whose name colliding here sent greps to the wrong concept — walk find,
    /// 2026-08-13.)
    fn sample(&mut self) -> Option<u64>;
}

impl PressureSource for Box<dyn crate::source::Source> {
    fn sample(&mut self) -> Option<u64> {
        // The governor's signal is the FOREIGN-pressure counter, not the batch
        // loop's own-extraction counter: a keyset export's own pages inflate
        // the spill counters by design, and a governor listening to them sheds
        // its own workers to the floor and never recovers (field find,
        // 2026-08-13 — see `Source::sample_governor_pressure`).
        crate::source::Source::sample_governor_pressure(self.as_mut())
    }
}

/// The adaptive concurrency governor — the inline `thread::scope` closure
/// that used to live in [`crate::pipeline::chunked::exec::run_chunked_parallel`]
/// turned into a self-contained, testable abstraction.
///
/// Why a struct (not just functions): the decision policy
/// ([`GovernorState`]) and the sample cadence (sample/poll intervals,
/// `RIVET_GOVERNOR_INTERVAL_MS` env override) are runtime-coupled — the
/// poll interval must be clamped to the sample interval, and the decision
/// state is mutated across ticks. Bundling them into one type makes the
/// "what to test, what to fake" boundary obvious: the source is the
/// dependency, the runner-side side effects are a callback.
pub struct Governor {
    state: GovernorState,
    sample_interval: Duration,
    poll_interval: Duration,
}

impl Governor {
    /// Build a governor that starts at `start`, clamped into `[floor,
    /// ceiling]`, and uses the env-tunable sample cadence
    /// (`RIVET_GOVERNOR_INTERVAL_MS`; falls back to
    /// [`GOVERNOR_SAMPLE_INTERVAL_MS`]). The poll interval is clamped to
    /// the sample interval so a tiny override (used in deterministic live
    /// tests) actually polls that fast, instead of being capped at the
    /// default [`GOVERNOR_POLL_MS`].
    pub fn new(start: usize, floor: usize, ceiling: usize) -> Self {
        let sample_ms = sample_interval_ms_from_env();
        let poll_ms = GOVERNOR_POLL_MS.min(sample_ms);
        Self {
            state: GovernorState::new(start, floor, ceiling),
            sample_interval: Duration::from_millis(sample_ms),
            poll_interval: Duration::from_millis(poll_ms),
        }
    }

    /// Build a governor with explicit intervals — bypasses the env-var
    /// read so unit tests can drive the loop deterministically without
    /// mutating process-global state.
    #[cfg(test)]
    pub fn with_intervals(
        start: usize,
        floor: usize,
        ceiling: usize,
        sample_interval: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            state: GovernorState::new(start, floor, ceiling),
            sample_interval,
            poll_interval,
        }
    }

    /// Record the harness's up-front arm probe — see
    /// [`GovernorState::note_arm_probe`]. Call it once, before [`run`](Self::run),
    /// with whether that probe could READ the pressure signal.
    pub fn note_arm_probe(&mut self, readable: bool) {
        self.state.note_arm_probe(readable);
    }

    /// Pure decision step: fold one sample into the state. Returns
    /// `Some((from, to))` on a parallelism transition, `None` otherwise.
    /// Mirrors [`GovernorState::observe`]; exposed at the [`Governor`]
    /// surface so tests can drive the policy without entering `run`.
    pub fn tick(&mut self, sample: Option<u64>) -> Option<(usize, usize)> {
        self.state.observe(sample)
    }

    /// Drive the sample loop until `stop` returns true. On every
    /// parallelism transition the `on_decision(from, to, cause)` callback
    /// fires — the runner binds it to its semaphore-resize + log +
    /// decision-log machinery, and the [`DecisionCause`] is what keeps its
    /// log line and journal reason honest (a step taken while blind is a
    /// fail-open, not "source pressure eased"). Polls every `poll_interval`,
    /// samples every `sample_interval`. The stop predicate is re-checked after
    /// each poll sleep so a finished run exits within one poll quantum.
    ///
    /// `on_blind_signal(kind, frozen_at, ceiling)` fires ONCE per blindness
    /// episode, on the tick where [`GOVERNOR_BLIND_SAMPLES`] consecutive
    /// samples have come back unreadable — `frozen_at` is the level the
    /// run had been pinned at, BEFORE this tick's step back toward the
    /// ceiling, and `kind` says whether the signal DIED or was never
    /// readable at all ([`BlindSignal`]). It is a separate callback (not
    /// another `on_decision` cause) because there is no transition to
    /// journal when the governor is already at the ceiling, and the
    /// operator still needs the line.
    pub fn run<S, Stop, Decide, Blind>(
        &mut self,
        source: &mut S,
        stop: Stop,
        mut on_decision: Decide,
        mut on_blind_signal: Blind,
    ) where
        S: PressureSource + ?Sized,
        Stop: Fn() -> bool,
        Decide: FnMut(usize, usize, DecisionCause),
        Blind: FnMut(BlindSignal, usize, usize),
    {
        let mut last_sample = Instant::now();
        while !stop() {
            std::thread::sleep(self.poll_interval);
            if stop() {
                break;
            }
            if last_sample.elapsed() < self.sample_interval {
                continue;
            }
            last_sample = Instant::now();
            let frozen_at = self.state.current;
            let decision = self.tick(source.sample());
            if let Some(kind) = self.state.take_blind_report() {
                on_blind_signal(kind, frozen_at, self.state.ceiling);
            }
            if let Some((from, to)) = decision {
                on_decision(from, to, self.decision_cause(from, to));
            }
        }
    }

    /// Classify a transition the state just produced. Pure and separate from
    /// [`run`](Self::run) so both the blind fail-open and the two pressure
    /// causes are unit-provable without threads: BLINDNESS wins over the
    /// direction of the step, because while blind the direction carries no
    /// information about the source at all.
    fn decision_cause(&self, from: usize, to: usize) -> DecisionCause {
        if self.state.is_blind() {
            DecisionCause::SignalUnreadable
        } else if to < from {
            DecisionCause::PressureRising
        } else {
            DecisionCause::PressureEased
        }
    }
}

/// Read `RIVET_GOVERNOR_INTERVAL_MS` and fall back to the production default.
/// Lives next to [`Governor`] so live tests and the production runner share
/// one resolution path — extracted from the inline read in
/// `run_chunked_parallel` so tests can verify the cadence policy.
fn sample_interval_ms_from_env() -> u64 {
    std::env::var("RIVET_GOVERNOR_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(GOVERNOR_SAMPLE_INTERVAL_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_shrinks_by_25_percent_under_pressure() {
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, true), 7_500);
        assert_eq!(next_adaptive_batch_size(8_000, 10_000, true), 6_000);
    }

    #[test]
    fn adaptive_grows_by_25_percent_when_idle() {
        // 4_000 × 5/4 = 5_000; well under base ceiling.
        assert_eq!(next_adaptive_batch_size(4_000, 10_000, false), 5_000);
    }

    #[test]
    fn adaptive_recovery_caps_at_base_ceiling() {
        // 9_000 × 5/4 = 11_250, but base is 10_000 — must clamp.
        assert_eq!(next_adaptive_batch_size(9_000, 10_000, false), 10_000);
        // Already at base: stays there.
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, false), 10_000);
    }

    #[test]
    fn adaptive_shrink_respects_min_floor() {
        // 600 × 3/4 = 450, but ADAPTIVE_MIN_BATCH = 500 — must clamp up.
        assert_eq!(
            next_adaptive_batch_size(600, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
        // Already at floor: stays at floor.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_pressure_path_ignores_base_uses_only_floor() {
        // Pressure path never consults base: shrink is computed from current,
        // then clamped only to ADAPTIVE_MIN_BATCH. A pathologically low base
        // does not artificially pin us lower than the floor.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 100, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_steady_state_oscillation_stays_bounded() {
        // Simulate 50 sample cycles under sustained pressure, then sustained recovery.
        // Verifies: the loop never wanders below floor or above base, and converges.
        let base = 5_000;
        let mut s = base;
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, true);
        }
        assert_eq!(
            s, ADAPTIVE_MIN_BATCH,
            "sustained pressure must converge to floor"
        );
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, false);
        }
        assert_eq!(s, base, "sustained recovery must converge to base ceiling");
    }

    // ── next_parallel (governor) ──────────────────────────────────────────────

    #[test]
    fn next_parallel_sheds_one_under_pressure() {
        assert_eq!(next_parallel(8, 1, 8, true), 7);
        assert_eq!(next_parallel(4, 1, 8, true), 3);
    }

    #[test]
    fn next_parallel_recovers_one_when_idle() {
        assert_eq!(next_parallel(4, 1, 8, false), 5);
    }

    #[test]
    fn next_parallel_shrink_respects_min_floor() {
        assert_eq!(next_parallel(2, 2, 8, true), 2, "already at min stays");
        assert_eq!(next_parallel(1, 1, 8, true), 1, "never below 1");
    }

    #[test]
    fn next_parallel_grow_respects_max_ceiling() {
        assert_eq!(next_parallel(8, 1, 8, false), 8, "already at max stays");
    }

    #[test]
    fn next_parallel_min_floored_at_one() {
        // A nonsensical min=0 must not let the count drop to 0 (would stall).
        assert_eq!(next_parallel(1, 0, 8, true), 1);
    }

    #[test]
    fn next_parallel_steady_state_converges_to_bounds() {
        let (min, max) = (2, 6);
        let mut p = max;
        for _ in 0..20 {
            p = next_parallel(p, min, max, true);
        }
        assert_eq!(p, min, "sustained pressure converges to min");
        for _ in 0..20 {
            p = next_parallel(p, min, max, false);
        }
        assert_eq!(p, max, "sustained recovery converges to max");
    }

    // ── GovernorState ─────────────────────────────────────────────────────────

    #[test]
    fn governor_state_clamps_start_into_bounds() {
        assert_eq!(GovernorState::new(99, 2, 6).current(), 6);
        assert_eq!(GovernorState::new(0, 2, 6).current(), 2);
        // floor floored at 1, ceiling at 1.
        assert_eq!(GovernorState::new(5, 0, 0).current(), 1);
    }

    #[test]
    fn governor_state_first_sample_only_sets_baseline_then_recovers() {
        // Start at ceiling: first idle sample wants to grow but is already
        // capped, so no change is reported.
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(100)), None, "at ceiling, idle ⇒ no change");
        assert_eq!(g.current(), 6);
    }

    #[test]
    fn governor_state_backs_off_under_rising_pressure() {
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(100)), None); // baseline, at ceiling
        assert_eq!(g.observe(Some(200)), Some((6, 5)), "rising ⇒ shed one");
        assert_eq!(g.observe(Some(300)), Some((5, 4)));
        assert_eq!(g.current(), 4);
    }

    #[test]
    fn governor_state_recovers_when_pressure_flat() {
        let mut g = GovernorState::new(3, 2, 6);
        assert_eq!(
            g.observe(Some(100)),
            Some((3, 4)),
            "flat/idle ⇒ recover one"
        );
        assert_eq!(g.observe(Some(100)), Some((4, 5)));
    }

    /// Policy pin, not a bug report (#229 bughunt finding [2]): the shed is
    /// CHEAP TO UNDO by construction — one rising sample sheds a worker and the
    /// very next FLAT sample hands it straight back. On a coarse counter (PG's
    /// `checkpoints_req` ticks roughly once a minute against a 1500 ms sample
    /// interval) sustained foreign pressure therefore reads as a repeated
    /// one-worker dip, NOT a held-down level.
    ///
    /// That is the deliberate trade: the regression this governor was rewritten
    /// for was a shed that would not come back (4→3→2→1 off its own exhaust,
    /// 1h48m lost in the field). Hysteresis would deepen the back-off on a
    /// coarse signal at the cost of making every engine's false positive
    /// stickier. Keep it here as an executable statement of the policy: adding
    /// hysteresis turns this RED, which is exactly the review the change
    /// deserves. Two rise→flat cycles, because a single one cannot show that
    /// the dip REPEATS rather than converging.
    #[test]
    fn governor_state_returns_the_shed_worker_on_the_first_flat_sample_by_design() {
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(10)), None, "baseline, already at ceiling");
        assert_eq!(g.observe(Some(11)), Some((6, 5)), "one rise ⇒ shed one");
        assert_eq!(g.observe(Some(11)), Some((5, 6)), "flat ⇒ back at once");
        assert_eq!(g.observe(Some(11)), None, "at ceiling, nothing to recover");
        assert_eq!(
            g.observe(Some(12)),
            Some((6, 5)),
            "next tick of the counter"
        );
        assert_eq!(g.observe(Some(12)), Some((5, 6)), "and back again");
    }

    #[test]
    fn governor_state_none_sample_holds_flat_and_keeps_baseline() {
        let mut g = GovernorState::new(4, 2, 6);
        assert_eq!(g.observe(Some(200)), Some((4, 5))); // baseline=200, grew
        assert_eq!(g.observe(None), None, "no sample ⇒ no change");
        // Baseline stayed 200, so a later 300 still reads as rising.
        assert_eq!(
            g.observe(Some(300)),
            Some((5, 4)),
            "rising vs preserved baseline"
        );
    }

    /// A transient miss holds parallelism flat; a PERMANENT one (monitor
    /// connection reaped mid-run — every later sample `None`) must not leave the
    /// run pinned at the shed floor for hours. After
    /// [`GOVERNOR_BLIND_SAMPLES`] the state warns ONCE and fails open.
    ///
    /// Two blindness episodes, because the warn's one-shot flag accumulates: a
    /// single episode cannot distinguish "one warn per episode" from "one warn
    /// per process".
    #[test]
    fn governor_state_holds_flat_for_a_transient_miss_then_fails_open_when_the_signal_dies() {
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(100)), None, "baseline at ceiling");
        assert_eq!(g.observe(Some(200)), Some((6, 5)));
        assert_eq!(g.observe(Some(300)), Some((5, 4)));

        assert_eq!(g.observe(None), None, "miss 1 ⇒ hold flat");
        assert_eq!(
            g.take_blind_report(),
            None,
            "one miss is a transient, not a loss"
        );
        assert_eq!(g.observe(None), None, "miss 2 ⇒ still holding");
        assert_eq!(g.take_blind_report(), None);
        assert_eq!(
            g.observe(None),
            Some((4, 5)),
            "miss 3 ⇒ the signal is gone; an unreadable signal is not evidence \
             of pressure, so step back toward the ceiling"
        );
        assert_eq!(
            g.take_blind_report(),
            Some(BlindSignal::Lost),
            "the signal WAS readable here, so this is a loss — the operator gets a line"
        );
        assert_eq!(g.observe(None), Some((5, 6)), "keeps stepping back");
        assert_eq!(
            g.take_blind_report(),
            None,
            "one warn per blindness episode, not one per sample interval"
        );
        assert_eq!(
            g.observe(None),
            None,
            "at the ceiling there is nowhere left"
        );

        // The baseline was preserved across the blind span, so the first
        // readable sample is still compared against the last real one (300).
        assert_eq!(g.observe(Some(400)), Some((6, 5)), "rising vs the baseline");
        assert_eq!(g.observe(None), None);
        assert_eq!(g.observe(None), None);
        assert_eq!(
            g.observe(None),
            Some((5, 6)),
            "second episode fails open too"
        );
        assert_eq!(
            g.take_blind_report(),
            Some(BlindSignal::Lost),
            "a NEW episode re-arms the warn"
        );
    }

    /// The trajectory the loss report must NOT claim: a signal that was never
    /// readable AT ALL (MSSQL without `VIEW SERVER STATE`, a MySQL build
    /// without `Innodb_log_waits`, MongoDB which never overrides the
    /// `sample_governor_pressure` default). Nothing died, nothing was pinned —
    /// the harness's arm probe already said so once, correctly. Before the fix
    /// this state was indistinguishable from a mid-run death: `observe` set the
    /// flag on `blind == GOVERNOR_BLIND_SAMPLES` without ever asking whether
    /// there had been a reading, so every run of such an engine warned "governor
    /// LOST its pressure signal … pinned at 6 of 6; stepping back toward 6"
    /// ~4.5 s after the probe line that said the opposite.
    ///
    /// Fixture starts BELOW the ceiling and runs past the report so the fail-open
    /// half is graded too (it is unchanged: the report is a REPORT), and keeps
    /// feeding `None` afterwards because the flag is a one-shot that accumulates
    /// — one blind span cannot tell "once per episode" from "once per tick".
    #[test]
    fn governor_state_never_reports_a_loss_for_a_signal_that_was_never_readable() {
        let mut g = GovernorState::new(4, 2, 6);
        assert_eq!(g.observe(None), None, "miss 1 ⇒ hold flat");
        assert_eq!(g.take_blind_report(), None);
        assert_eq!(g.observe(None), None, "miss 2 ⇒ still holding");
        assert_eq!(g.take_blind_report(), None);
        assert_eq!(
            g.observe(None),
            Some((4, 5)),
            "miss 3 ⇒ fail open exactly as for a lost signal"
        );
        assert_eq!(
            g.take_blind_report(),
            Some(BlindSignal::NeverReadable),
            "the signal was never read on this run — reporting it as a LOSS tells the \
             operator a healthy monitoring connection died"
        );
        assert_eq!(g.observe(None), Some((5, 6)), "keeps failing open");
        assert_eq!(g.take_blind_report(), None, "one report per episode");
        assert_eq!(
            g.observe(None),
            None,
            "at the ceiling there is nowhere left"
        );
        assert_eq!(g.take_blind_report(), None);
    }

    /// The arm probe is the OTHER way a signal counts as having been readable,
    /// and it is not `prev`: `GovernorHarness::spawn_into` deliberately does not
    /// feed its probe's VALUE to the baseline, so a monitor connection that
    /// answered the probe and died before the loop's first sample leaves
    /// `prev == None` forever. That is a real loss and must read as one — gating
    /// the report on `prev.is_some()` (the obvious one-line fix) would silence it.
    ///
    /// Both trajectories from ONE fixture shape, differing only in the probe.
    #[test]
    fn governor_state_arm_probe_readability_decides_loss_versus_never_readable() {
        for (probe_readable, want) in [
            (true, BlindSignal::Lost),
            (false, BlindSignal::NeverReadable),
        ] {
            let mut g = GovernorState::new(6, 2, 6);
            g.note_arm_probe(probe_readable);
            assert_eq!(g.observe(None), None, "miss 1 (probe={probe_readable})");
            assert_eq!(g.observe(None), None, "miss 2 (probe={probe_readable})");
            assert_eq!(g.observe(None), None, "miss 3: already at the ceiling");
            assert_eq!(
                g.take_blind_report(),
                Some(want),
                "a probe that could read the signal makes a later blind span a LOSS \
                 (probe={probe_readable})"
            );
        }
    }

    /// A step taken while BLIND is a fail-open, not evidence about the source —
    /// so it must not be labelled `source pressure eased: recovered` in the log
    /// line or the run journal's `ParallelismAdjusted` reason. Two blind steps,
    /// because the cause is derived per transition.
    #[test]
    fn governor_state_is_blind_only_after_the_threshold_of_consecutive_misses() {
        let mut g = GovernorState::new(4, 2, 6);
        assert!(!g.is_blind(), "fresh state is not blind");
        g.observe(Some(100));
        assert!(!g.is_blind(), "a readable sample is not blind");
        g.observe(None);
        g.observe(None);
        assert!(!g.is_blind(), "below the threshold it is a transient miss");
        g.observe(None);
        assert!(g.is_blind(), "at the threshold the loop is failing open");
        g.observe(None);
        assert!(g.is_blind(), "and stays blind while the misses continue");
        g.observe(Some(200));
        assert!(!g.is_blind(), "a readable sample clears it");
    }

    // ── Governor (the loop, not just the decision) ────────────────────────────

    /// In-memory [`PressureSource`] that hands out canned samples in order
    /// and reports `None` once exhausted (mimics a source that lost its
    /// connection mid-run). Bumps a shared counter on every call so the
    /// test's `stop` predicate can fire after a fixed number of samples
    /// — keying off decisions instead would deadlock the loop when the
    /// first sample only sets the baseline (no decision → no signal).
    struct VecSource {
        samples: std::collections::VecDeque<Option<u64>>,
        sample_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl VecSource {
        fn new(
            samples: impl IntoIterator<Item = Option<u64>>,
            sample_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        ) -> Self {
            Self {
                samples: samples.into_iter().collect(),
                sample_count,
            }
        }
    }

    impl PressureSource for VecSource {
        fn sample(&mut self) -> Option<u64> {
            self.sample_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.samples.pop_front().unwrap_or(None)
        }
    }

    #[test]
    fn governor_tick_mirrors_governor_state_observe() {
        // The `tick` method must be a faithful surface for the policy; if it
        // ever diverges from GovernorState::observe, the runner-side
        // unit-test guarantee breaks. Drive both with the same sequence and
        // assert identical outputs.
        let samples = [Some(100u64), Some(200), Some(150), None, Some(400)];
        let mut g =
            Governor::with_intervals(6, 2, 6, Duration::from_millis(1), Duration::from_millis(1));
        let mut s = GovernorState::new(6, 2, 6);
        for sample in samples {
            assert_eq!(g.tick(sample), s.observe(sample));
        }
    }

    #[test]
    fn governor_run_emits_decisions_for_every_rising_sample_until_stop() {
        // Polls at 1 ms, samples at 1 ms — every wake fires a sample so the
        // 5 canned samples produce exactly the 4 transitions the policy
        // would emit (the first sample only sets the baseline, hence one
        // fewer decision than samples). Stop predicate is keyed on the
        // *sample* counter — keying it on decisions would deadlock because
        // the first sample never emits one.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sample_count = Arc::new(AtomicUsize::new(0));
        let mut source = VecSource::new(
            [
                Some(100),
                Some(200), // rising → 6→5
                Some(300), // rising → 5→4
                Some(400), // rising → 4→3
                Some(500), // rising → 3→2 (clamped at floor)
            ],
            Arc::clone(&sample_count),
        );
        let mut gov =
            Governor::with_intervals(6, 2, 6, Duration::from_millis(1), Duration::from_millis(1));
        let stop_count = Arc::clone(&sample_count);
        let stop = move || stop_count.load(Ordering::Relaxed) >= 5;
        let mut decisions: Vec<(usize, usize, DecisionCause)> = Vec::new();
        gov.run(
            &mut source,
            stop,
            |from, to, cause| decisions.push((from, to, cause)),
            |_, _, _| panic!("the signal never went blind here"),
        );

        // First sample (100) only seeds the baseline → no decision.
        // Samples 2..5 all rise → four shed-one decisions.
        assert_eq!(
            decisions,
            vec![
                (6, 5, DecisionCause::PressureRising),
                (5, 4, DecisionCause::PressureRising),
                (4, 3, DecisionCause::PressureRising),
                (3, 2, DecisionCause::PressureRising),
            ]
        );
    }

    /// The loop half of the blind-signal policy: a monitor connection that dies
    /// mid-run (`VecSource` returns `None` forever once exhausted — the same
    /// shape as `postgres::Client` after a pooler reap, which never reconnects)
    /// must not leave the run pinned at the level the last shed reached. The
    /// governor warns once, naming the FROZEN level (the value before this
    /// tick's recovery step), and walks back to the ceiling.
    #[test]
    fn governor_run_warns_once_and_steps_back_to_the_ceiling_when_the_signal_dies() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sample_count = Arc::new(AtomicUsize::new(0));
        let mut source = VecSource::new(
            [
                Some(100), // baseline (already at the ceiling)
                Some(200), // rising → 6→5
                Some(300), // rising → 5→4
                None,      // miss 1 → hold
                None,      // miss 2 → hold
                None,      // miss 3 → signal lost: warn + 4→5
                None,      // still blind → 5→6
            ],
            Arc::clone(&sample_count),
        );
        let mut gov =
            Governor::with_intervals(6, 2, 6, Duration::from_millis(1), Duration::from_millis(1));
        let stop_count = Arc::clone(&sample_count);
        let stop = move || stop_count.load(Ordering::Relaxed) >= 7;
        let mut decisions: Vec<(usize, usize, DecisionCause)> = Vec::new();
        let mut lost: Vec<(BlindSignal, usize, usize)> = Vec::new();
        gov.run(
            &mut source,
            stop,
            |from, to, cause| decisions.push((from, to, cause)),
            |kind, frozen_at, ceiling| lost.push((kind, frozen_at, ceiling)),
        );

        assert_eq!(
            decisions,
            vec![
                (6, 5, DecisionCause::PressureRising),
                (5, 4, DecisionCause::PressureRising),
                // NOT `PressureEased`: while blind, the direction of the step says
                // nothing about the source — journalling "source pressure eased:
                // recovered" here is a statement about a signal nobody could read.
                (4, 5, DecisionCause::SignalUnreadable),
                (5, 6, DecisionCause::SignalUnreadable),
            ],
            "two sheds, then the blind span walks back to the ceiling"
        );
        assert_eq!(
            lost,
            vec![(BlindSignal::Lost, 4, 6)],
            "exactly one warn — a signal that WAS read (samples 1-3) and stopped is a LOSS, \
             named with the level the run was pinned at and the ceiling"
        );
    }

    /// The loop half of the OTHER blind trajectory: an engine whose sampler
    /// returns `None` from the very first tick (MSSQL without `VIEW SERVER
    /// STATE`, MongoDB, a MySQL build without `Innodb_log_waits`). It must fail
    /// open exactly like a lost signal but report `NeverReadable`, so the runner
    /// does not print a death notice for a connection that is perfectly healthy
    /// and was already reported by the arm probe.
    ///
    /// Runs past the report (two blind steps) so the one-shot is graded, and
    /// starts below the ceiling so the fail-open produces visible transitions.
    #[test]
    fn governor_run_reports_a_never_readable_signal_as_such_and_still_fails_open() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sample_count = Arc::new(AtomicUsize::new(0));
        // `VecSource` yields `None` once exhausted, so an empty deque is a
        // sampler that never reads anything — the production shape here.
        let mut source = VecSource::new([], Arc::clone(&sample_count));
        let mut gov =
            Governor::with_intervals(4, 2, 6, Duration::from_millis(1), Duration::from_millis(1));
        let stop_count = Arc::clone(&sample_count);
        let stop = move || stop_count.load(Ordering::Relaxed) >= 4;
        let mut decisions: Vec<(usize, usize, DecisionCause)> = Vec::new();
        let mut blind: Vec<(BlindSignal, usize, usize)> = Vec::new();
        gov.run(
            &mut source,
            stop,
            |from, to, cause| decisions.push((from, to, cause)),
            |kind, frozen_at, ceiling| blind.push((kind, frozen_at, ceiling)),
        );

        assert_eq!(
            decisions,
            vec![
                (4, 5, DecisionCause::SignalUnreadable),
                (5, 6, DecisionCause::SignalUnreadable),
            ],
            "fail-open is identical for a never-readable signal — only the REPORT differs"
        );
        assert_eq!(
            blind,
            vec![(BlindSignal::NeverReadable, 4, 6)],
            "the signal never existed on this run; calling that a LOSS contradicts the \
             harness's own arm-probe line"
        );
    }

    #[test]
    fn governor_run_stops_promptly_within_one_poll_quantum() {
        // `stop` flips before the first sleep returns; `run` must observe it
        // immediately on the next stop-check rather than waiting a full
        // sample interval. Asserts the loop honours the stop predicate as a
        // hot exit condition (the deadlock-class bug from 16fc662 would
        // re-surface here as a non-terminating test if regressed).
        let sample_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut source = VecSource::new([Some(100)], sample_count);
        let mut gov =
            Governor::with_intervals(6, 2, 6, Duration::from_millis(50), Duration::from_millis(5));
        let start = std::time::Instant::now();
        gov.run(&mut source, || true, |_, _, _| {}, |_, _, _| {});
        assert!(
            start.elapsed() < Duration::from_millis(40),
            "run() must exit on stop without sleeping a full sample interval"
        );
    }
}
