use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
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
    unsafe {
        let mut info: libc::mach_task_basic_info_data_t = mem::zeroed();
        let mut count = (mem::size_of::<libc::mach_task_basic_info_data_t>()
            / mem::size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
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
