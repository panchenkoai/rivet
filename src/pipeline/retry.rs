//! **Layer: Execution**
//!
//! Error classification for retry logic.  `classify_error` maps raw error strings
//! to retry categories (transient vs permanent, reconnect-needed, extra delay).
//! No plan data is read here — this is pure error-signal processing.

/// Classifies transient errors into retry categories.
/// Returns (is_transient, needs_reconnect, extra_delay_ms)
///
/// Checks Postgres SQLSTATE codes and MySQL error codes first, then falls back
/// to string matching for errors that don't carry structured codes (e.g. IO,
/// cloud credential errors).
pub fn classify_error(err: &anyhow::Error) -> (bool, bool, u64) {
    // --- Postgres: check SQLSTATE via the `postgres::Error` downcasted type ---
    if let Some(pg) = err.downcast_ref::<postgres::Error>() {
        if let Some(db) = pg.as_db_error() {
            return classify_pg_sqlstate(db.code());
        }
        // Connection-level (non-DB) postgres errors → reconnect
        if pg.is_closed() {
            return (true, true, 0);
        }
    }

    // --- MySQL: check numeric error code ---
    if let Some(result) = err
        .downcast_ref::<mysql::Error>()
        .and_then(classify_mysql_error)
    {
        return result;
    }

    // --- Fallback: string-based classification ---
    let msg = format!("{:#}", err).to_lowercase();

    // Auth / credential errors are never transient — fix config, not retry
    if msg.contains("loading credential")
        || msg.contains("loadcredential")
        || msg.contains("metadata.google.internal")
        || msg.contains("permission denied")
        || msg.contains("access denied")
        || msg.contains("invalid_grant")
        || msg.contains("token has been expired or revoked")
    {
        return (false, false, 0);
    }

    // Network errors -- need reconnect
    if msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("connection refused")
        || msg.contains("no route to host")
        || msg.contains("network is unreachable")
        || msg.contains("name resolution")
        || msg.contains("dns")
        || msg.contains("ssl handshake")
        || msg.contains("i/o timeout")
        || msg.contains("unexpected eof")
        || msg.contains("closed the connection unexpectedly")
        || msg.contains("got an error reading communication packets")
    {
        return (true, true, 0);
    }

    // MySQL specific -- need reconnect
    if msg.contains("gone away")
        || msg.contains("lost connection")
        || msg.contains("the server closed the connection")
        || msg.contains("can't connect to mysql server")
    {
        return (true, true, 0);
    }

    // Timeout errors -- retry on same connection
    if msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("canceling statement")
        || msg.contains("lock wait timeout")
        || msg.contains("execution time exceeded")
    {
        return (true, false, 0);
    }

    // Capacity errors -- retry with longer delay
    if msg.contains("too many connections")
        || msg.contains("the database system is starting up")
        || msg.contains("the database system is shutting down")
    {
        return (true, true, 15_000);
    }

    // Deadlock/serialization -- retry once, same connection
    if msg.contains("deadlock") || msg.contains("could not serialize access") {
        return (true, false, 1_000);
    }

    // Not transient
    (false, false, 0)
}

/// Classify a Postgres SQLSTATE code.
/// Reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>
fn classify_pg_sqlstate(code: &postgres::error::SqlState) -> (bool, bool, u64) {
    use postgres::error::SqlState;

    // Class 08 — Connection Exception → reconnect
    if *code == SqlState::CONNECTION_EXCEPTION
        || *code == SqlState::CONNECTION_DOES_NOT_EXIST
        || *code == SqlState::CONNECTION_FAILURE
        || *code == SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        || *code == SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        || code.code().starts_with("08")
    {
        return (true, true, 0);
    }

    // 57P01 admin_shutdown, 57P02 crash_shutdown, 57P03 cannot_connect_now
    if *code == SqlState::ADMIN_SHUTDOWN
        || *code == SqlState::CRASH_SHUTDOWN
        || *code == SqlState::CANNOT_CONNECT_NOW
    {
        return (true, true, 15_000);
    }

    // 53300 too_many_connections
    if *code == SqlState::TOO_MANY_CONNECTIONS {
        return (true, true, 15_000);
    }

    // 40001 serialization_failure, 40P01 deadlock_detected
    if *code == SqlState::T_R_SERIALIZATION_FAILURE {
        return (true, false, 1_000);
    }
    if *code == SqlState::T_R_DEADLOCK_DETECTED {
        return (true, false, 1_000);
    }

    // 57014 query_canceled (statement_timeout)
    if *code == SqlState::QUERY_CANCELED {
        return (true, false, 0);
    }

    // Class 53 — Insufficient Resources (disk full, out of memory)
    if code.code().starts_with("53") {
        return (true, false, 5_000);
    }

    // 28xxx — Invalid Authorization → permanent
    if code.code().starts_with("28") {
        return (false, false, 0);
    }

    // 42xxx — Syntax Error or Access Rule Violation → permanent
    if code.code().starts_with("42") {
        return (false, false, 0);
    }

    // All other SQLSTATE codes → not transient by default
    (false, false, 0)
}

/// Classify a MySQL error by numeric code.
/// Reference: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>
fn classify_mysql_error(err: &mysql::Error) -> Option<(bool, bool, u64)> {
    match err {
        mysql::Error::MySqlError(me) => {
            match me.code {
                // ER_LOCK_DEADLOCK
                1213 => Some((true, false, 1_000)),
                // ER_LOCK_WAIT_TIMEOUT
                1205 => Some((true, false, 0)),
                // ER_CON_COUNT_ERROR (too many connections)
                1040 => Some((true, true, 15_000)),
                // ER_SERVER_SHUTDOWN
                1053 => Some((true, true, 15_000)),
                // ER_ACCESS_DENIED_ERROR, ER_DBACCESS_DENIED_ERROR
                1045 | 1044 => Some((false, false, 0)),
                // ER_BAD_DB_ERROR, ER_NO_SUCH_TABLE, ER_PARSE_ERROR
                1049 | 1146 | 1064 => Some((false, false, 0)),
                _ => None,
            }
        }
        mysql::Error::IoError(_) => Some((true, true, 0)),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn is_transient(err: &anyhow::Error) -> bool {
    classify_error(err).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_transient_matches() {
        assert!(is_transient(&anyhow::anyhow!("statement timed out")));
        assert!(is_transient(&anyhow::anyhow!("connection reset")));
    }

    #[test]
    fn test_is_transient_rejects() {
        assert!(!is_transient(&anyhow::anyhow!("syntax error")));
        assert!(!is_transient(&anyhow::anyhow!("permission denied")));
        assert!(!is_transient(&anyhow::anyhow!("table not found")));
    }

    #[test]
    fn test_classify_network_errors_need_reconnect() {
        let cases = [
            "connection refused",
            "no route to host",
            "network is unreachable",
            "broken pipe",
            "unexpected eof",
            "MySQL server has gone away",
            "lost connection to server",
            "can't connect to mysql server",
            "the server closed the connection",
            "got an error reading communication packets",
            "ssl handshake failed",
        ];
        for msg in cases {
            let (transient, reconnect, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(transient, "should be transient: {}", msg);
            assert!(reconnect, "should need reconnect: {}", msg);
        }
    }

    #[test]
    fn test_classify_timeout_no_reconnect() {
        let (t, r, _) = classify_error(&anyhow::anyhow!("statement timed out"));
        assert!(t);
        assert!(!r, "timeout should not require reconnect");

        let (t, r, _) = classify_error(&anyhow::anyhow!("lock wait timeout exceeded"));
        assert!(t);
        assert!(!r);
    }

    #[test]
    fn test_classify_capacity_errors_extra_delay() {
        let (t, r, delay) = classify_error(&anyhow::anyhow!("too many connections"));
        assert!(t);
        assert!(r);
        assert!(
            delay >= 10_000,
            "capacity errors should have extra delay, got: {}ms",
            delay
        );

        let (t, _, delay) = classify_error(&anyhow::anyhow!("the database system is starting up"));
        assert!(t);
        assert!(delay >= 10_000);
    }

    #[test]
    fn test_classify_deadlock_retryable() {
        let (t, r, delay) = classify_error(&anyhow::anyhow!("deadlock detected"));
        assert!(t);
        assert!(!r, "deadlock should not require reconnect");
        assert!(delay >= 1_000, "deadlock should have small extra delay");
    }

    #[test]
    fn test_classify_permanent_errors() {
        let cases = [
            "syntax error",
            "permission denied",
            "relation does not exist",
            "column not found",
        ];
        for msg in cases {
            let (transient, _, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(!transient, "should NOT be transient: {}", msg);
        }
    }

    #[test]
    fn test_classify_credential_errors_not_transient() {
        let cases = [
            "loading credential to sign http request",
            "error sending request for url (http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token): dns error",
            "invalid_grant: Token has been expired or revoked",
            "Access Denied: no permission",
        ];
        for msg in cases {
            let (transient, _, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(
                !transient,
                "credential error should NOT be transient: {}",
                msg
            );
        }
    }
}
