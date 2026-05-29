use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

/// Background thread polls RSS while an export runs so `peak_rss_mb` reflects in-process highs,
/// not only values at start/end (which often miss parallel workers' allocations).
pub struct RssPeakSampler {
    peak: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RssPeakSampler {
    /// Spawns a sampler every `interval_ms` (default-quality tradeoff: 100ms).
    /// `seed_mb` is folded into the peak (typically RSS immediately before work starts).
    pub fn start(seed_mb: usize, interval_ms: u64) -> Self {
        let peak = Arc::new(AtomicUsize::new(seed_mb));
        let stop = Arc::new(AtomicBool::new(false));
        let peak_c = Arc::clone(&peak);
        let stop_c = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("rivet-rss-peak".into())
            .spawn(move || {
                while !stop_c.load(Ordering::Relaxed) {
                    let r = get_rss_mb();
                    peak_c.fetch_max(r, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(interval_ms));
                }
                let r = get_rss_mb();
                peak_c.fetch_max(r, Ordering::Relaxed);
            })
            .expect("spawn rss peak sampler");
        Self {
            peak,
            stop,
            handle: Some(handle),
        }
    }

    /// Stops sampling and returns the highest RSS (MB) seen, including a final read.
    pub fn stop(mut self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        let last = get_rss_mb();
        self.peak.load(Ordering::Relaxed).max(last)
    }
}

/// Returns the current process RSS in megabytes, or 0 if unavailable.
pub fn get_rss_mb() -> usize {
    #[cfg(target_os = "macos")]
    {
        macos_rss_mb()
    }
    #[cfg(target_os = "linux")]
    {
        linux_rss_mb()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

#[cfg(target_os = "macos")]
fn macos_rss_mb() -> usize {
    use std::mem;
    // SAFETY: `task_info` is a stable macOS kernel API.  We zero-initialise the
    // struct, pass the correct `count` (struct size / natural_t), and check the
    // return code before reading the result.  No aliasing or lifetime issues.
    unsafe {
        let mut info: libc::mach_task_basic_info_data_t = mem::zeroed();
        let mut count = (mem::size_of::<libc::mach_task_basic_info_data_t>()
            / mem::size_of::<libc::natural_t>())
            as libc::mach_msg_type_number_t;
        let kr = libc::task_info(
            mach2::traps::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            &mut info as *mut _ as libc::task_info_t,
            &mut count,
        );
        if kr == libc::KERN_SUCCESS {
            (info.resident_size as usize) / (1024 * 1024)
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_rss_mb() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map(|pages| pages * 4096 / (1024 * 1024))
        .unwrap_or(0)
}

pub fn check_memory(threshold_mb: usize) -> bool {
    if threshold_mb == 0 {
        return true;
    }
    let rss = get_rss_mb();
    if rss > threshold_mb {
        log::warn!("RSS {}MB exceeds threshold {}MB", rss, threshold_mb);
        return false;
    }
    true
}

/// Counting semaphore built on `Mutex + Condvar` so blocked acquirers park in
/// the kernel rather than spinning on an atomic.
///
/// Replaces the prior pattern in `pipeline::chunked::exec`:
/// ```ignore
/// while atomic.load(Relaxed) >= max { thread::sleep(50ms); }
/// atomic.fetch_add(1, Relaxed);
/// ```
/// which polled 20 times/sec per blocked worker. Under N parallel chunks and a
/// long-running export this burned ~N × 20 wake-ups per second doing nothing.
///
/// With `Condvar::wait`, blocked threads consume zero CPU until a `release()`
/// notifies. The mutex is uncontended whenever traffic is light, so the lock
/// path adds no measurable overhead vs the atomic.
///
/// The permit ceiling is mutable at runtime via [`Semaphore::resize`] so the
/// OPT-2 concurrency governor can back parallelism off (and recover it) under
/// source pressure without tearing down the worker pool.
pub struct Semaphore {
    state: std::sync::Mutex<SemState>,
    cond: std::sync::Condvar,
}

struct SemState {
    /// Permits currently held by live acquirers.
    count: usize,
    /// Permit ceiling. Mutable via `resize`.
    max: usize,
}

impl Semaphore {
    pub fn new(max: usize) -> Self {
        Self {
            state: std::sync::Mutex::new(SemState { count: 0, max }),
            cond: std::sync::Condvar::new(),
        }
    }

    /// Block until a permit is available, then take one.
    pub fn acquire(&self) {
        let mut st = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while st.count >= st.max {
            st = self
                .cond
                .wait(st)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        st.count += 1;
    }

    /// Return a permit and wake one blocked acquirer (if any).
    pub fn release(&self) {
        let mut st = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(st.count > 0, "release without matching acquire");
        st.count -= 1;
        self.cond.notify_one();
    }

    /// Change the permit ceiling at runtime.
    ///
    /// Raising it wakes every parked acquirer so the freshly available permits
    /// are taken promptly. Lowering it takes effect lazily: in-flight permits
    /// are honored to completion, and new `acquire()` calls block until `count`
    /// falls below the new ceiling. The caller is responsible for keeping
    /// `new_max >= 1` (a `0` ceiling parks all acquirers indefinitely).
    pub fn resize(&self, new_max: usize) {
        let mut st = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let raised = new_max > st.max;
        st.max = new_max;
        if raised {
            self.cond.notify_all();
        }
    }

    /// The current permit ceiling. Test-only observability accessor.
    #[cfg(test)]
    pub fn current_max(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_memory_zero_threshold_always_passes() {
        assert!(check_memory(0));
    }

    #[test]
    fn check_memory_huge_threshold_passes() {
        // No test process will reach 1 TB of RAM.
        assert!(check_memory(1_024 * 1_024));
    }

    #[test]
    fn get_rss_mb_does_not_panic() {
        let _ = get_rss_mb();
    }

    #[test]
    fn rss_peak_sampler_stop_returns_value() {
        let sampler = RssPeakSampler::start(0, 50);
        let _peak = sampler.stop();
    }

    #[test]
    fn rss_peak_sampler_seed_is_lower_bound() {
        let high_seed = 9999;
        let sampler = RssPeakSampler::start(high_seed, 50);
        let peak = sampler.stop();
        assert!(peak >= high_seed);
    }

    // ── Semaphore ───────────────────────────────────────────────────────────

    #[test]
    fn semaphore_admits_up_to_max_without_blocking() {
        let sem = Semaphore::new(3);
        sem.acquire();
        sem.acquire();
        sem.acquire();
        // Three permits taken, no deadlock so far → invariant holds.
        sem.release();
        sem.release();
        sem.release();
    }

    #[test]
    fn semaphore_blocks_fourth_until_release() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let sem = Arc::new(Semaphore::new(2));
        sem.acquire();
        sem.acquire();

        let entered = Arc::new(AtomicBool::new(false));
        let entered_w = Arc::clone(&entered);
        let sem_w = Arc::clone(&sem);
        let handle = std::thread::spawn(move || {
            sem_w.acquire();
            entered_w.store(true, Ordering::Release);
            sem_w.release();
        });

        // Worker is parked in `acquire()` — give it a moment and confirm.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !entered.load(Ordering::Acquire),
            "worker must be blocked while 2/2 permits are taken"
        );

        // Releasing one permit must wake the worker.
        sem.release();
        handle.join().expect("worker thread");
        assert!(
            entered.load(Ordering::Acquire),
            "worker should have entered after release"
        );
        sem.release();
    }

    #[test]
    fn semaphore_current_max_reports_resize() {
        let sem = Semaphore::new(2);
        assert_eq!(sem.current_max(), 2);
        sem.resize(5);
        assert_eq!(sem.current_max(), 5);
        sem.resize(1);
        assert_eq!(sem.current_max(), 1);
    }

    #[test]
    fn semaphore_resize_up_wakes_parked_acquirer() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        // 1/1 permits taken → a second acquirer parks.
        let sem = Arc::new(Semaphore::new(1));
        sem.acquire();

        let entered = Arc::new(AtomicBool::new(false));
        let entered_w = Arc::clone(&entered);
        let sem_w = Arc::clone(&sem);
        let handle = std::thread::spawn(move || {
            sem_w.acquire();
            entered_w.store(true, Ordering::Release);
            sem_w.release();
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !entered.load(Ordering::Acquire),
            "worker must be parked while 1/1 permits are taken"
        );

        // Raising the ceiling (not a release) must wake the parked worker.
        sem.resize(2);
        handle.join().expect("worker thread");
        assert!(
            entered.load(Ordering::Acquire),
            "raising the ceiling should admit the parked worker"
        );
        sem.release();
    }

    #[test]
    fn semaphore_resize_down_blocks_new_acquire_until_count_drops() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        // 2/2 taken, then shrink ceiling to 1.
        let sem = Arc::new(Semaphore::new(2));
        sem.acquire();
        sem.acquire();
        sem.resize(1);

        // Releasing one leaves count=1, which is still >= the new max=1, so a
        // fresh acquirer must block.
        sem.release();

        let entered = Arc::new(AtomicBool::new(false));
        let entered_w = Arc::clone(&entered);
        let sem_w = Arc::clone(&sem);
        let handle = std::thread::spawn(move || {
            sem_w.acquire();
            entered_w.store(true, Ordering::Release);
            sem_w.release();
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !entered.load(Ordering::Acquire),
            "count(1) >= new max(1): acquirer must block after shrink"
        );

        // Dropping count to 0 frees a slot under the lowered ceiling. The woken
        // worker then takes and returns that permit itself, so accounting stays
        // balanced (3 acquires / 3 releases) — no trailing release here.
        sem.release();
        handle.join().expect("worker thread");
        assert!(
            entered.load(Ordering::Acquire),
            "acquirer should proceed once count falls below the new ceiling"
        );
    }
}
