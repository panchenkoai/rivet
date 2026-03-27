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
