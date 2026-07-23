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

/// Ceiling on a single retry's exponential backoff. Without a cap the wait
/// doubles unboundedly, so a `max_retries` set high enough to ride out a
/// minutes-long flaky-tunnel outage becomes impractical (one retry would sleep
/// for hours) — and `2u64.pow(attempt - 1)` PANICS once `attempt - 1 >= 64`, so a
/// large `max_retries` aborted the export outright. Capping each wait keeps a
/// generous retry budget usable and the arithmetic overflow-free.
pub const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

/// Backoff (ms) for retry `attempt` (1-based): `base * 2^(attempt-1)`, clamped to
/// [`MAX_RETRY_BACKOFF_MS`], then `extra` added. Saturating throughout so a large
/// `attempt` can neither overflow nor panic. Shared by every per-attempt retry
/// loop (single + parallel-checkpoint) so their backoff can't drift apart.
pub fn retry_backoff_ms(base_ms: u64, attempt: u32, extra_ms: u64) -> u64 {
    base_ms
        .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)))
        .min(MAX_RETRY_BACKOFF_MS)
        .saturating_add(extra_ms)
}

/// Classifies transient errors into retry categories.
///
/// Order: a typed [`crate::source::StatementDurationTimeout`] marker
/// (rivet-raised, robust to wording), then structured Postgres SQLSTATE /
/// MySQL error codes, then a string fallback for errors that carry no
/// structured signal (IO, cloud credentials, driver-native timeout prose).
pub fn classify_error(err: &anyhow::Error) -> RetryClass {
    // --- Typed marker: rivet-raised statement-duration timeout (deterministic) ---
    // Downcast the TYPE so permanence does not depend on the Display wording.
    if err
        .downcast_ref::<crate::source::StatementDurationTimeout>()
        .is_some()
    {
        return PERMANENT;
    }

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

    // --- MongoDB: network / server-selection / retryable-read command codes ---
    if let Some(result) = err
        .downcast_ref::<mongodb::error::Error>()
        .and_then(classify_mongo_error)
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
    if msg.contains("statement timeout after")      // rivet MSSQL message
        || msg.contains("due to statement timeout")  // PG statement_timeout
        || msg.contains("execution time exceeded")   // MySQL max_execution_time
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

    // 55P03 lock_not_available — a `lock_timeout` fired while waiting on a
    // row/table lock (rivet sets `SET LOCAL lock_timeout` per tuning). Unlike a
    // statement-*duration* timeout this is genuinely transient: the blocking
    // transaction may have committed by the next attempt, so retry on the same
    // connection. Classified here on the structured-error path because
    // `classify_error` short-circuits to `classify_pg_sqlstate` for any
    // `postgres::Error` with a db_error, making the string-path
    // "lock wait timeout" branch unreachable for a real PG lock_timeout.
    if *code == SqlState::LOCK_NOT_AVAILABLE {
        return TRANSIENT_SAME_CONN;
    }

    // 57014 query_canceled — in an export this means `statement_timeout` fired
    // (rivet never user-cancels mid-export). The statement exceeded its
    // duration budget; retrying the identical query re-fails identically, so
    // propagate immediately rather than burn the budget again (3× by default).
    // Distinct from the lock_timeout (55P03, just above) and deadlock (40P01,
    // above) cases, which stay transient because the contention clears.
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
            // ER_QUERY_TIMEOUT (3024): `max_execution_time` / `MAX_EXECUTION_TIME`
            // fired — a deterministic statement-DURATION timeout. The identical
            // query re-times-out, so retrying just burns the budget again.
            // Classified by code here so permanence does not depend on the
            // English server message reaching the string path below.
            3024 => Some(PERMANENT),
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

/// Classify a MongoDB driver error — the typed primary path, mirroring the
/// Postgres SQLSTATE / MySQL-code classifiers above (the string fallback is only
/// a backup). The driver's own retryable-read handling (`retryReads`, on by
/// default) retries a transient read exactly ONCE; this lets rivet's chunk retry
/// loop ride out failures *past* that single attempt.
///
/// Coverage note: the `Io` / `DnsResolve` / `ServerSelection` branches OVERLAP
/// the string fallback (a dropped socket renders "connection reset" / "unexpected
/// eof", which the string path already catches — verified: the toxiproxy
/// mid-scan test passes even with this arm stubbed out). The branch that is NOT
/// redundant is the retryable-read COMMAND CODES: a replica-set failover mid-scan
/// surfaces `ErrorKind::Command{code: 189/10107/11602/…}` ("not primary",
/// "interrupted due to repl state change") whose wording the string path does
/// NOT match — without this arm those abort the export as `PERMANENT`. Toxiproxy
/// injects network faults, not server command errors, so that branch is covered
/// by reasoning + the spec code list, not an isolating live test (and
/// `ErrorKind` is `#[non_exhaustive]`, so it cannot be unit-constructed).
///
/// The read-retryable codes mirror the MongoDB spec; the driver's own list
/// (`RETRYABLE_READ_CODES`) is `pub(crate)`, so it is duplicated here.
fn classify_mongo_error(err: &mongodb::error::Error) -> Option<RetryClass> {
    use mongodb::error::ErrorKind;
    const RETRYABLE_READ_CODES: &[i32] = &[
        6,     // HostUnreachable
        7,     // HostNotFound
        89,    // NetworkTimeout
        91,    // ShutdownInProgress
        134,   // ReadConcernMajorityNotAvailableYet
        189,   // PrimarySteppedDown
        262,   // ExceededTimeLimit
        9001,  // SocketException
        10107, // NotWritablePrimary
        11600, // InterruptedAtShutdown
        11602, // InterruptedDueToReplStateChange
        13435, // NotPrimaryNoSecondaryOk
        13436, // NotPrimaryOrSecondary
    ];
    match &*err.kind {
        // A dropped/reset socket or a DNS blip — the connection is gone.
        ErrorKind::Io(_) | ErrorKind::DnsResolve { .. } => Some(TRANSIENT_RECONNECT),
        // No server selectable right now (primary election, all nodes down).
        ErrorKind::ServerSelection { .. } => Some(TRANSIENT_RECONNECT),
        // Pool cleared after a network error — a fresh connection is needed.
        ErrorKind::ConnectionPoolCleared { .. } => Some(TRANSIENT_RECONNECT),
        // A server command error is retryable only for the read-retryable codes
        // (stepdown, shutdown-in-progress, not-primary, socket exception, …).
        ErrorKind::Command(ce) if RETRYABLE_READ_CODES.contains(&ce.code) => {
            Some(TRANSIENT_RECONNECT)
        }
        // Auth never fixes itself — retrying just burns the budget.
        ErrorKind::Authentication { .. } => Some(PERMANENT),
        // Everything else (malformed command, decode error, rivet-side shutdown)
        // falls through to the string path / default.
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
    fn retry_backoff_doubles_then_caps_and_never_overflows() {
        // Exponential doubling below the cap.
        assert_eq!(retry_backoff_ms(5_000, 1, 0), 5_000);
        assert_eq!(retry_backoff_ms(5_000, 2, 0), 10_000);
        assert_eq!(retry_backoff_ms(5_000, 3, 0), 20_000);
        assert_eq!(retry_backoff_ms(5_000, 4, 0), 40_000);
        // 5_000 * 2^4 = 80_000 → clamped to the 60s ceiling.
        assert_eq!(retry_backoff_ms(5_000, 5, 0), MAX_RETRY_BACKOFF_MS);
        assert_eq!(retry_backoff_ms(5_000, 10, 0), MAX_RETRY_BACKOFF_MS);
        // extra_delay is added AFTER the cap.
        assert_eq!(retry_backoff_ms(5_000, 10, 500), MAX_RETRY_BACKOFF_MS + 500);
        // The load-bearing robustness property: a large attempt must NOT panic
        // (the old `2u64.pow(attempt - 1)` aborted the export at attempt ≥ 65).
        assert_eq!(retry_backoff_ms(5_000, 100, 0), MAX_RETRY_BACKOFF_MS);
        assert_eq!(retry_backoff_ms(u64::MAX, u32::MAX, u64::MAX), u64::MAX);
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

    /// Real PG errors carry a SQLSTATE, so `classify_error` short-circuits to
    /// `classify_pg_sqlstate` — the string-path lock-wait branch above is
    /// unreachable for them. This exercises the *reachable* PG classification
    /// directly: 55P03 lock_timeout is transient (contention clears), 57014
    /// statement_timeout is permanent (deterministic), 40P01 deadlock transient.
    #[test]
    fn pg_sqlstate_lock_timeout_transient_but_statement_timeout_permanent() {
        use postgres::error::SqlState;
        assert_eq!(
            classify_pg_sqlstate(&SqlState::LOCK_NOT_AVAILABLE),
            TRANSIENT_SAME_CONN,
            "PG 55P03 lock_timeout must stay retryable"
        );
        assert_eq!(
            classify_pg_sqlstate(&SqlState::QUERY_CANCELED),
            PERMANENT,
            "PG 57014 statement_timeout must not be retried"
        );
        assert!(
            classify_pg_sqlstate(&SqlState::T_R_DEADLOCK_DETECTED).is_transient(),
            "PG 40P01 deadlock must stay retryable"
        );
    }

    /// WIP-completion / roast finding: MySQL `max_execution_time` raises
    /// ER_QUERY_TIMEOUT (3024) as a STRUCTURED error. Classifying it by numeric
    /// code (not the English message) makes permanence robust to a reworded /
    /// localized server string. Today 3024 is unhandled in classify_mysql_error
    /// (returns None) and falls to the string path, where a message containing
    /// "timed out" is wrongly classified TRANSIENT_SAME_CONN — exactly the
    /// 3×budget-burn the duration-timeout rule exists to prevent.
    #[test]
    fn test_mysql_query_timeout_code_3024_is_permanent() {
        let err = mysql::Error::MySqlError(mysql::error::MySqlError {
            state: "HY000".to_string(),
            // A reworded message that contains a transient needle ("timed out")
            // but NOT the English duration needles — proves we classify on the
            // CODE, not the prose.
            message: "Query interrupted: timed out".to_string(),
            code: 3024,
        });
        let c = classify_error(&anyhow::Error::new(err));
        assert_eq!(
            c, PERMANENT,
            "MySQL ER_QUERY_TIMEOUT (3024) is a deterministic duration timeout — must be permanent regardless of message wording"
        );
    }

    /// The typed [`StatementDurationTimeout`] marker classifies PERMANENT via
    /// downcast — BEFORE the string path runs. Proven robust here: even when the
    /// error is wrapped in anyhow context (as it is on the way up the export
    /// stack), the type survives and wins over any wording. Guards the MSSQL
    /// fix against a future message reword silently flipping it to transient.
    #[test]
    fn test_statement_duration_timeout_typed_marker_is_permanent() {
        use crate::source::StatementDurationTimeout;
        let typed: anyhow::Error = StatementDurationTimeout::mssql(300).into();
        assert_eq!(
            classify_error(&typed),
            PERMANENT,
            "typed duration-timeout marker must be permanent"
        );
        // Survives context wrapping (anyhow downcast walks the chain).
        let wrapped = typed.context("export 'orders': chunk 4 failed");
        assert_eq!(
            classify_error(&wrapped),
            PERMANENT,
            "typed marker must survive anyhow context wrapping"
        );
        // The user-facing Display still carries the actionable remediation.
        let msg = format!("{}", StatementDurationTimeout::mssql(300));
        assert!(
            msg.contains("mode: chunked") && msg.contains("statement_timeout_s"),
            "Display must keep the actionable hint: {msg}"
        );
    }

    /// Belt-and-suspenders: the string fallback still classifies a genuinely
    /// driver-native duration-timeout message (one we do NOT raise ourselves)
    /// as permanent, so engines without a typed marker stay covered.
    #[test]
    fn test_native_duration_timeout_string_still_permanent() {
        for msg in [
            "canceling statement due to statement timeout",
            "Query execution was interrupted, maximum statement execution time exceeded",
        ] {
            assert_eq!(
                classify_error(&anyhow::anyhow!("{}", msg)),
                PERMANENT,
                "native duration-timeout string must stay permanent: {msg}"
            );
        }
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
