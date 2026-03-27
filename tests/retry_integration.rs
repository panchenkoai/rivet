use rivet::pipeline::classify_error;

// ─── classify_error coverage for all patterns ────────────────

#[test]
fn network_errors_are_transient_and_need_reconnect() {
    let cases = [
        "connection reset by peer",
        "broken pipe",
        "connection refused",
        "no route to host",
        "network is unreachable",
        "name resolution failed",
        "dns lookup failed",
        "ssl handshake failed",
        "i/o timeout",
        "unexpected eof",
        "server closed the connection unexpectedly",
        "got an error reading communication packets",
    ];
    for msg in cases {
        let (transient, reconnect, delay) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(transient, "'{}' should be transient", msg);
        assert!(reconnect, "'{}' should need reconnect", msg);
        assert_eq!(delay, 0, "'{}' should have no extra delay", msg);
    }
}

#[test]
fn mysql_disconnect_errors_need_reconnect() {
    let cases = [
        "MySQL server has gone away",
        "lost connection to MySQL server during query",
        "the server closed the connection",
        "can't connect to mysql server on 'localhost'",
    ];
    for msg in cases {
        let (transient, reconnect, _) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(transient, "'{}' should be transient", msg);
        assert!(reconnect, "'{}' should need reconnect", msg);
    }
}

#[test]
fn timeout_errors_retry_without_reconnect() {
    let cases = [
        "statement timed out",
        "canceling statement due to statement timeout",
        "lock wait timeout exceeded; try restarting transaction",
        "query execution was interrupted, maximum execution time exceeded",
    ];
    for msg in cases {
        let (transient, reconnect, delay) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(transient, "'{}' should be transient", msg);
        assert!(!reconnect, "'{}' should NOT need reconnect", msg);
        assert_eq!(delay, 0, "'{}' should have no extra delay", msg);
    }
}

#[test]
fn capacity_errors_have_extra_delay() {
    let cases = [
        ("FATAL: too many connections for role \"rivet\"", 15_000),
        ("the database system is starting up", 15_000),
        ("the database system is shutting down", 15_000),
    ];
    for (msg, expected_min_delay) in cases {
        let (transient, reconnect, delay) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(transient, "'{}' should be transient", msg);
        assert!(reconnect, "'{}' should need reconnect", msg);
        assert!(
            delay >= expected_min_delay,
            "'{}' should have delay >= {}ms, got {}ms",
            msg, expected_min_delay, delay
        );
    }
}

#[test]
fn deadlock_errors_retry_with_small_delay() {
    let cases = [
        "deadlock detected",
        "Deadlock found when trying to get lock; try restarting transaction",
        "could not serialize access due to concurrent update",
    ];
    for msg in cases {
        let (transient, reconnect, delay) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(transient, "'{}' should be transient", msg);
        assert!(!reconnect, "'{}' should NOT need reconnect (same tx)", msg);
        assert!(delay >= 1_000, "'{}' should have delay >= 1000ms, got {}ms", msg, delay);
    }
}

#[test]
fn permanent_errors_not_retried() {
    let cases = [
        "ERROR: syntax error at or near \"SELCT\"",
        "ERROR: permission denied for table users",
        "ERROR: relation \"nonexistent\" does not exist",
        "ERROR: column \"foo\" does not exist",
        "Unknown column 'bar' in 'field list'",
        "Access denied for user 'rivet'@'localhost'",
        "ERROR: invalid input syntax for type integer: \"abc\"",
        "Table 'mydb.missing_table' doesn't exist",
    ];
    for msg in cases {
        let (transient, _, _) = classify_error(&anyhow::anyhow!("{}", msg));
        assert!(!transient, "'{}' should NOT be transient", msg);
    }
}

// ─── Mixed case and substring matching ───────────────────────

#[test]
fn case_insensitive_matching() {
    let (t, _, _) = classify_error(&anyhow::anyhow!("CONNECTION RESET BY PEER"));
    assert!(t, "should match case-insensitively");

    let (t, _, _) = classify_error(&anyhow::anyhow!("MySQL Server Has Gone Away"));
    assert!(t, "should match mixed case");
}

#[test]
fn embedded_in_longer_message() {
    let (t, r, _) = classify_error(&anyhow::anyhow!(
        "db error: ERROR: the database system is starting up (PG server restarting after crash recovery)"
    ));
    assert!(t, "should match substring in longer message");
    assert!(r, "should need reconnect");
}

// ─── Edge cases ──────────────────────────────────────────────

#[test]
fn empty_error_not_transient() {
    let (t, _, _) = classify_error(&anyhow::anyhow!(""));
    assert!(!t);
}

#[test]
fn generic_io_error_not_transient() {
    let (t, _, _) = classify_error(&anyhow::anyhow!("file not found: config.yaml"));
    assert!(!t, "filesystem errors should not be transient");
}

// ─── Verify total pattern count ──────────────────────────────

#[test]
fn all_documented_patterns_covered() {
    let transient_patterns = [
        // Network
        "connection reset", "broken pipe", "connection refused",
        "no route to host", "network is unreachable", "name resolution",
        "dns", "ssl handshake", "i/o timeout", "unexpected eof",
        "closed the connection unexpectedly",
        "got an error reading communication packets",
        // MySQL
        "gone away", "lost connection", "the server closed the connection",
        "can't connect to mysql server",
        // Timeout
        "timed out", "timeout", "canceling statement", "lock wait timeout",
        "execution time exceeded",
        // Capacity
        "too many connections", "the database system is starting up",
        "the database system is shutting down",
        // Deadlock
        "deadlock", "could not serialize access",
    ];

    for pattern in transient_patterns {
        let (t, _, _) = classify_error(&anyhow::anyhow!("error: {}", pattern));
        assert!(t, "pattern '{}' should be recognized as transient", pattern);
    }
}
