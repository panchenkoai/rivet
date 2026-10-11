#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    rivet::cli::run_binary();
}

#[cfg(test)]
mod tests {
    static ROUTED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    /// A route that keeps every line and leaves none to stderr.
    fn keep(line: String) -> bool {
        ROUTED.lock().unwrap().push(line);
        true
    }

    /// The installed process logger sends an error line through the route it was given, its password redacted.
    #[test]
    fn the_installed_logger_sends_a_redacted_line_through_its_route() {
        rivet::redact::install_logger(keep);
        log::error!("dial postgresql://u:hunter2@h/db failed");
        let routed = ROUTED.lock().unwrap();
        assert_eq!(routed.len(), 1, "one line through the route: {routed:?}");
        assert!(
            routed[0].contains("dial postgresql://") && !routed[0].contains("hunter2"),
            "{routed:?}"
        );
    }
}
