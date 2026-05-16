use rivet::pipeline::classify_error;

fn classify(msg: &str) -> rivet::pipeline::RetryClass {
    classify_error(&anyhow::anyhow!("{}", msg))
}

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
        let c = classify(msg);
        assert!(c.is_transient(), "'{}' should be transient", msg);
        assert!(c.needs_reconnect(), "'{}' should need reconnect", msg);
        assert_eq!(
            c.extra_delay_ms(),
            0,
            "'{}' should have no extra delay",
            msg
        );
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
        let c = classify(msg);
        assert!(c.is_transient(), "'{}' should be transient", msg);
        assert!(c.needs_reconnect(), "'{}' should need reconnect", msg);
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
        let c = classify(msg);
        assert!(c.is_transient(), "'{}' should be transient", msg);
        assert!(!c.needs_reconnect(), "'{}' should NOT need reconnect", msg);
        assert_eq!(
            c.extra_delay_ms(),
            0,
            "'{}' should have no extra delay",
            msg
        );
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
        let c = classify(msg);
        assert!(c.is_transient(), "'{}' should be transient", msg);
        assert!(c.needs_reconnect(), "'{}' should need reconnect", msg);
        assert!(
            c.extra_delay_ms() >= expected_min_delay,
            "'{}' should have delay >= {}ms, got {}ms",
            msg,
            expected_min_delay,
            c.extra_delay_ms()
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
        let c = classify(msg);
        assert!(c.is_transient(), "'{}' should be transient", msg);
        assert!(
            !c.needs_reconnect(),
            "'{}' should NOT need reconnect (same tx)",
            msg
        );
        assert!(
            c.extra_delay_ms() >= 1_000,
            "'{}' should have delay >= 1000ms, got {}ms",
            msg,
            c.extra_delay_ms()
        );
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
        assert!(
            !classify(msg).is_transient(),
            "'{}' should NOT be transient",
            msg
        );
    }
}

// ─── Mixed case and substring matching ───────────────────────

#[test]
fn case_insensitive_matching() {
    assert!(
        classify("CONNECTION RESET BY PEER").is_transient(),
        "should match case-insensitively"
    );
    assert!(
        classify("MySQL Server Has Gone Away").is_transient(),
        "should match mixed case"
    );
}

#[test]
fn embedded_in_longer_message() {
    let c = classify(
        "db error: ERROR: the database system is starting up (PG server restarting after crash recovery)",
    );
    assert!(c.is_transient(), "should match substring in longer message");
    assert!(c.needs_reconnect(), "should need reconnect");
}

// ─── Edge cases ──────────────────────────────────────────────

#[test]
fn empty_error_not_transient() {
    assert!(!classify("").is_transient());
}

#[test]
fn generic_io_error_not_transient() {
    assert!(
        !classify("file not found: config.yaml").is_transient(),
        "filesystem errors should not be transient"
    );
}

// ─── Verify total pattern count ──────────────────────────────

#[test]
fn all_documented_patterns_covered() {
    let transient_patterns = [
        // Network
        "connection reset",
        "broken pipe",
        "connection refused",
        "no route to host",
        "network is unreachable",
        "name resolution",
        "dns",
        "ssl handshake",
        "i/o timeout",
        "unexpected eof",
        "closed the connection unexpectedly",
        "got an error reading communication packets",
        // MySQL
        "gone away",
        "lost connection",
        "the server closed the connection",
        "can't connect to mysql server",
        // Timeout
        "timed out",
        "timeout",
        "canceling statement",
        "lock wait timeout",
        "execution time exceeded",
        // Capacity
        "too many connections",
        "the database system is starting up",
        "the database system is shutting down",
        // Deadlock
        "deadlock",
        "could not serialize access",
    ];

    for pattern in transient_patterns {
        let c = classify(&format!("error: {pattern}"));
        assert!(
            c.is_transient(),
            "pattern '{}' should be recognized as transient",
            pattern
        );
    }
}
