//! Smoke tests for the `resource` module (RSS sampling, memory threshold).
//!
//! Migrated from the former `tests/v2_golden.rs`.  These tests are platform-
//! sensitive but intentionally loose: they only assert the module returns
//! something sane on macOS / Linux, not exact byte counts.

#[test]
fn get_rss_returns_nonzero_value() {
    let rss = rivet::resource::get_rss_mb();
    assert!(rss > 0, "RSS should be > 0 on macOS/Linux, got: {rss}");
}

#[test]
fn rss_peak_sampler_stops_cleanly_and_reports_at_least_seed() {
    let seed = rivet::resource::get_rss_mb();
    let s = rivet::resource::RssPeakSampler::start(seed, 10);
    std::thread::sleep(std::time::Duration::from_millis(40));
    let peak = s.stop();
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        assert!(
            peak >= seed,
            "peak should be at least seed, got {peak} < {seed}"
        );
    }
    // On other platforms the sampler may no-op; we only assert it returns.
    let _ = peak;
}

#[test]
fn check_memory_with_zero_threshold_is_disabled() {
    assert!(rivet::resource::check_memory(0));
}

#[test]
fn check_memory_with_high_threshold_passes() {
    assert!(rivet::resource::check_memory(100_000));
}
