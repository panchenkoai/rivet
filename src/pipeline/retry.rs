//! **Layer: Execution**
//!
//! Error classification for retry logic.  `classify_error` maps raw error strings
//! to retry categories (transient vs permanent, reconnect-needed, extra delay).
//! No plan data is read here — this is pure error-signal processing.

/// Outcome of `classify_error`.
///
/// Replaces the prior `(bool, bool, u64)` tuple. Callers used positional
/// destructuring (`let (transient, _, _) = ...`) which made it easy to
/// confuse the two booleans. With an enum the type system forces a `match`
/// or one of the named accessors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    /// Fatal error: retrying will not help (auth failure, bad SQL, etc.).
    /// The retry loop should propagate immediately.
    Permanent,
    /// Worth another attempt. Carries advice for the caller.
    Transient {
        /// If true, the existing source connection is suspect (e.g. network
        /// reset, "gone away") and the caller must open a fresh one before
        /// retrying. If false, the same connection is fine to reuse.
        needs_reconnect: bool,
        /// Extra delay (ms) on top of the standard exponential backoff —
        /// used for capacity / rate-limit errors that benefit from a longer
        /// settling period than a transient network blip.
        extra_delay_ms: u64,
    },
}

impl RetryClass {
    pub fn is_transient(self) -> bool {
        matches!(self, RetryClass::Transient { .. })
    }

    pub fn needs_reconnect(self) -> bool {
        matches!(
            self,
            RetryClass::Transient {
                needs_reconnect: true,
                ..
            }
        )
    }

    pub fn extra_delay_ms(self) -> u64 {
        match self {
            RetryClass::Transient { extra_delay_ms, .. } => extra_delay_ms,
            RetryClass::Permanent => 0,
        }
    }
}

// Shorthand internal constructors — the older code passed naked tuples and
// these keep the matchers below short without losing type safety.
const PERMANENT: RetryClass = RetryClass::Permanent;
const TRANSIENT_RECONNECT: RetryClass = RetryClass::Transient {
    needs_reconnect: true,
    extra_delay_ms: 0,
};
const TRANSIENT_SAME_CONN: RetryClass = RetryClass::Transient {
    needs_reconnect: false,
    extra_delay_ms: 0,
};
fn transient(needs_reconnect: bool, extra_delay_ms: u64) -> RetryClass {
    RetryClass::Transient {
        needs_reconnect,
        extra_delay_ms,
    }
}

/// Classifies transient errors into retry categories.
///
/// Checks Postgres SQLSTATE codes and MySQL error codes first, then falls back
/// to string matching for errors that don't carry structured codes (e.g. IO,
/// cloud credential errors).
pub fn classify_error(err: &anyhow::Error) -> RetryClass {
    // --- Postgres: check SQLSTATE via the `postgres::Error` downcasted type ---
    if let Some(pg) = err.downcast_ref::<postgres::Error>() {
        if let Some(db) = pg.as_db_error() {
            return classify_pg_sqlstate(db.code());
        }
        // Connection-level (non-DB) postgres errors → reconnect
        if pg.is_closed() {
            return TRANSIENT_RECONNECT;
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
        return PERMANENT;
    }

    // OpenDAL self-classified transient errors: when the underlying service
    // (GCS, S3, ...) returns a retryable failure OpenDAL renders the error
    // with the `(temporary)` qualifier — e.g.:
    //   "Unexpected (temporary) at write, context: { … } => send http request,
    //    source: error sending request: client error (SendRequest):
    //    dispatch task is gone: runtime dropped the dispatch task"
    // These are exactly what the chunk retry loop is designed to ride out;
    // before this branch was added, hyper/h2 task drops on long uploads
    // surfaced as permanent errors and aborted the chunk after a single try.
    if msg.contains("(temporary)")
        || msg.contains("dispatch task is gone")
        || msg.contains("runtime dropped the dispatch task")
        || (msg.contains("client error (sendrequest)") && msg.contains("send http request"))
    {
        // Treat as a brief network blip — no reconnect of a SQL session is
        // needed (this is the destination side) and a small extra delay
        // gives the http client time to reset its connection pool.
        return transient(false, 500);
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
        return TRANSIENT_RECONNECT;
    }

    // MySQL specific -- need reconnect
    if msg.contains("gone away")
        || msg.contains("lost connection")
        || msg.contains("the server closed the connection")
        || msg.contains("can't connect to mysql server")
    {
        return TRANSIENT_RECONNECT;
    }

    // Statement-DURATION timeout: the query exceeded its own time budget
    // (PG `statement_timeout`, MySQL `max_execution_time`, MSSQL's client-side
    // cap). This is *deterministic* on an unchunked scan — the identical query
    // re-times-out, so a retry just burns another full budget for nothing.
    // Measured: MSSQL full-mode over 2 M wide rows retried 3×300 s = 20 min to
    // fail with 0 rows (REPORT_full_vs_parallel.md). Treat as permanent; the
    // error message carries the actionable fix (split with `mode: chunked`, or
    // raise the budget). Distinct from a *lock-wait* timeout (the lock releases)
    // and a network i/o timeout (a blip) — both stay transient just below.
    if msg.contains("statement timeout after")                  // rivet MSSQL message
        || msg.contains("due to statement timeout")             // PG statement_timeout
        || msg.contains("maximum statement execution time exceeded") // MySQL max_execution_time
        || msg.contains("max_execution_time")
    {
        return PERMANENT;
    }

    // Lock-wait / network timeout errors -- genuinely transient (the condition
    // clears: a held lock releases, a network blip passes). Retry on same conn.
    if msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("canceling statement")
        || msg.contains("lock wait timeout")
    {
        return TRANSIENT_SAME_CONN;
    }

    // Capacity errors -- retry with longer delay
    if msg.contains("too many connections")
        || msg.contains("the database system is starting up")
        || msg.contains("the database system is shutting down")
    {
        return transient(true, 15_000);
    }

    // Deadlock/serialization -- retry once, same connection
    if msg.contains("deadlock") || msg.contains("could not serialize access") {
        return transient(false, 1_000);
    }

    // Not transient
    PERMANENT
}

/// Classify a Postgres SQLSTATE code.
/// Reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>
fn classify_pg_sqlstate(code: &postgres::error::SqlState) -> RetryClass {
    use postgres::error::SqlState;

    // Class 08 — Connection Exception → reconnect
    if *code == SqlState::CONNECTION_EXCEPTION
        || *code == SqlState::CONNECTION_DOES_NOT_EXIST
        || *code == SqlState::CONNECTION_FAILURE
        || *code == SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        || *code == SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        || code.code().starts_with("08")
    {
        return TRANSIENT_RECONNECT;
    }

    // 57P01 admin_shutdown, 57P02 crash_shutdown, 57P03 cannot_connect_now
    if *code == SqlState::ADMIN_SHUTDOWN
        || *code == SqlState::CRASH_SHUTDOWN
        || *code == SqlState::CANNOT_CONNECT_NOW
    {
        return transient(true, 15_000);
    }

    // 53300 too_many_connections
    if *code == SqlState::TOO_MANY_CONNECTIONS {
        return transient(true, 15_000);
    }

    // 40001 serialization_failure, 40P01 deadlock_detected
    if *code == SqlState::T_R_SERIALIZATION_FAILURE {
        return transient(false, 1_000);
    }
    if *code == SqlState::T_R_DEADLOCK_DETECTED {
        return transient(false, 1_000);
    }

    // 57014 query_canceled — in an export this means `statement_timeout` fired
    // (rivet never user-cancels mid-export). The statement exceeded its
    // duration budget; retrying the identical query re-fails identically, so
    // propagate immediately rather than burn the budget again (3× by default).
    // Lock timeouts are 55P03 and deadlocks 40P01 — handled separately, both
    // stay transient. See the string-path duration-timeout branch above.
    if *code == SqlState::QUERY_CANCELED {
        return PERMANENT;
    }

    // Class 53 — Insufficient Resources (disk full, out of memory)
    if code.code().starts_with("53") {
        return transient(false, 5_000);
    }

    // 28xxx — Invalid Authorization → permanent
    if code.code().starts_with("28") {
        return PERMANENT;
    }

    // 42xxx — Syntax Error or Access Rule Violation → permanent
    if code.code().starts_with("42") {
        return PERMANENT;
    }

    // All other SQLSTATE codes → not transient by default
    PERMANENT
}

/// Classify a MySQL error by numeric code.
/// Reference: <https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html>
fn classify_mysql_error(err: &mysql::Error) -> Option<RetryClass> {
    match err {
        mysql::Error::MySqlError(me) => match me.code {
            // ER_LOCK_DEADLOCK
            1213 => Some(transient(false, 1_000)),
            // ER_LOCK_WAIT_TIMEOUT
            1205 => Some(TRANSIENT_SAME_CONN),
            // ER_CON_COUNT_ERROR (too many connections) / ER_SERVER_SHUTDOWN
            1040 | 1053 => Some(transient(true, 15_000)),
            // ER_ACCESS_DENIED_ERROR, ER_DBACCESS_DENIED_ERROR
            // ER_BAD_DB_ERROR, ER_NO_SUCH_TABLE, ER_PARSE_ERROR
            1045 | 1044 | 1049 | 1146 | 1064 => Some(PERMANENT),
            _ => None,
        },
        mysql::Error::IoError(_) => Some(TRANSIENT_RECONNECT),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn is_transient(err: &anyhow::Error) -> bool {
    classify_error(err).is_transient()
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
            let c = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(c.is_transient(), "should be transient: {}", msg);
            assert!(c.needs_reconnect(), "should need reconnect: {}", msg);
        }
    }

    #[test]
    fn test_classify_timeout_no_reconnect() {
        let c = classify_error(&anyhow::anyhow!("statement timed out"));
        assert!(c.is_transient());
        assert!(!c.needs_reconnect(), "timeout should not require reconnect");

        let c = classify_error(&anyhow::anyhow!("lock wait timeout exceeded"));
        assert!(c.is_transient());
        assert!(!c.needs_reconnect());
    }

    /// A statement-DURATION timeout is deterministic on an unchunked scan:
    /// retrying the identical query re-times-out, so it must NOT be transient
    /// (measured: MSSQL full-mode retried 3×300 s = 20 min for 0 rows). The
    /// fix is `mode: chunked` or a bigger budget, never another attempt.
    #[test]
    fn test_statement_duration_timeout_is_permanent() {
        let cases = [
            // rivet's MSSQL client-side cap message (the measured bug)
            "mssql: statement timeout after 300s (tuning.statement_timeout_s) — use mode: chunked",
            // PG statement_timeout cancel text (if it reaches the string path)
            "canceling statement due to statement timeout",
            // MySQL max_execution_time
            "Query execution was interrupted, maximum statement execution time exceeded",
        ];
        for msg in cases {
            let c = classify_error(&anyhow::anyhow!("{}", msg));
            assert_eq!(c, PERMANENT, "duration timeout must be permanent: {msg}");
        }
    }

    /// A *lock-wait* timeout, by contrast, clears on its own — the lock is
    /// released — so it stays transient. Guards the split from the branch above.
    #[test]
    fn test_lock_wait_timeout_stays_transient() {
        let c = classify_error(&anyhow::anyhow!(
            "lock wait timeout exceeded; try restarting"
        ));
        assert!(c.is_transient(), "lock-wait timeout must remain retryable");
        assert!(!c.needs_reconnect());
    }

    #[test]
    fn test_classify_capacity_errors_extra_delay() {
        let c = classify_error(&anyhow::anyhow!("too many connections"));
        assert!(c.is_transient());
        assert!(c.needs_reconnect());
        assert!(
            c.extra_delay_ms() >= 10_000,
            "capacity errors should have extra delay, got: {}ms",
            c.extra_delay_ms()
        );

        let c = classify_error(&anyhow::anyhow!("the database system is starting up"));
        assert!(c.is_transient());
        assert!(c.extra_delay_ms() >= 10_000);
    }

    #[test]
    fn test_classify_deadlock_retryable() {
        let c = classify_error(&anyhow::anyhow!("deadlock detected"));
        assert!(c.is_transient());
        assert!(
            !c.needs_reconnect(),
            "deadlock should not require reconnect"
        );
        assert!(
            c.extra_delay_ms() >= 1_000,
            "deadlock should have small extra delay"
        );
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
            let c = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(!c.is_transient(), "should NOT be transient: {}", msg);
        }
    }

    #[test]
    fn test_classify_opendal_temporary_errors_retryable() {
        // The exact shape OpenDAL surfaces for transient cloud failures.  Without
        // this branch each chunk in a parallel checkpoint run would treat a
        // hyper dispatch-task drop as permanent and abort the whole worker
        // after one try, even though the upload usually succeeds on retry.
        let raw = "Unexpected (temporary) at write, context: { url: https://storage.googleapis.com/bucket/k.parquet?partNumber=1&uploadId=abc, called: http_util::Client::send } => send http request, source: error sending request: client error (SendRequest): dispatch task is gone: runtime dropped the dispatch task";
        let c = classify_error(&anyhow::anyhow!("{}", raw));
        assert!(c.is_transient(), "OpenDAL `(temporary)` must be retryable");
        assert!(
            !c.needs_reconnect(),
            "destination-side errors do not affect the SQL session"
        );
        assert!(
            c.extra_delay_ms() > 0,
            "give the http client a moment to reset its pool"
        );
    }

    #[test]
    fn test_classify_dispatch_task_gone_alone_retryable() {
        let c = classify_error(&anyhow::anyhow!(
            "client error (SendRequest): dispatch task is gone: runtime dropped"
        ));
        assert!(
            c.is_transient(),
            "bare hyper dispatch task drop is transient"
        );
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
            let c = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(
                !c.is_transient(),
                "credential error should NOT be transient: {}",
                msg
            );
        }
    }
}
