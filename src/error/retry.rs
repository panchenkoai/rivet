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
/// Order: a typed [`crate::source::StatementDurationTimeout`] marker
/// (rivet-raised, robust to wording), then structured Postgres SQLSTATE /
/// MySQL error codes, then a string fallback for errors that carry no
/// structured signal (IO, cloud credentials, driver-native timeout prose).
/// Should another attempt be made after a failure?
///
/// Budget left AND an error that can recover — the one rule every runner needs,
/// so it lives here rather than being re-typed per runner. It was written out
/// five times across three runners (`single`, chunked sequential, chunked
/// parallel, twice each for the connect and the export leg), each copy free to
/// drift and none of them tested. That is the shape this repo keeps paying for:
/// a rule each runner re-applies drifts one runner at a time.
///
/// `<`, not `<=`: `attempt` is zero-based, so `attempt == max_retries` is the
/// LAST permitted attempt and retrying from it runs `max_retries + 1` times.
/// `&&`, not `||`: a permanent failure (auth, TLS) would otherwise be retried
/// into the same wall while the operator waits out the full backoff for an
/// answer the first attempt already had.
pub struct Attempt<'a> {
    /// Zero-based index of the attempt that just failed.
    pub attempt: u32,
    /// Budget from the tuning profile.
    pub max_retries: u32,
    /// The failure being judged.
    pub error: &'a anyhow::Error,
}

pub fn should_retry(a: Attempt<'_>) -> bool {
    // Named rather than three positional arguments: `attempt` and `max_retries`
    // are both `u32`, so a swapped call site compiles, runs, and silently changes
    // the budget — the kind of edit no test distinguishes from the correct one.
    a.attempt < a.max_retries && classify_error(a.error).is_transient()
}

/// The retry class an `io::ErrorKind` proves: a dropped or refused connection is transient, a
/// file the operator must fix is permanent; any other kind (`Other`, a driver's wrapped error)
/// leaves the decision to the text.
fn classify_io_kind(kind: std::io::ErrorKind) -> Option<RetryClass> {
    use std::io::ErrorKind::*;
    match kind {
        ConnectionReset | ConnectionAborted | ConnectionRefused | NotConnected | BrokenPipe
        | TimedOut | UnexpectedEof | Interrupted | WouldBlock => Some(TRANSIENT_RECONNECT),
        NotFound | PermissionDenied | AlreadyExists | NotADirectory | IsADirectory
        | DirectoryNotEmpty | ReadOnlyFilesystem | StorageFull | FileTooLarge | InvalidInput
        | InvalidData | InvalidFilename | Unsupported => Some(PERMANENT),
        _ => None,
    }
}

pub fn classify_error(err: &anyhow::Error) -> RetryClass {
    // A typed stop (drift, integrity, a non-environment code) is permanent, whatever its text says.
    if crate::error::stop_class(err).is_some() {
        return PERMANENT;
    }
    // --- Typed marker: rivet-raised statement-duration timeout (deterministic) ---
    // Downcast the TYPE so permanence does not depend on the Display wording.
    if err
        .downcast_ref::<super::StatementDurationTimeout>()
        .is_some()
    {
        return PERMANENT;
    }

    // --- Typed marker: rivet stopped WAITING for a warehouse job it submitted ---
    // Same reasoning, opposite end of the pipeline: the wait already expired, so
    // repeating it only spends the budget again. Keyed on the TYPE for the same
    // reason — a reworded message must not be able to make it retryable.
    if err.downcast_ref::<super::JobWaitTimeout>().is_some() {
        return PERMANENT;
    }

    // --- Typed marker: the connect failed in the TLS handshake (config, not network) ---
    if err.downcast_ref::<super::TlsHandshakeFailed>().is_some() {
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

    // --- Object store: opendal's own verdict, before any text scan ---
    // The store's answer is final by TYPE: a 404 whose response headers happen to
    // carry `keep-alive: timeout=5` (Azurite) must not read as a timeout.
    if let Some(class) = err.chain().find_map(opendal_error).map(classify_opendal) {
        return class;
    }

    // --- A filesystem or socket error: its kind decides, before a path in the text can ---
    // The text names paths and tables an operator chose; a temp dir named `.tmprDnsA7` read
    // as a `dns` failure and turned a missing directory into a retry (exit 2 for 1).
    if let Some(class) = err
        .chain()
        .find_map(|e| e.downcast_ref::<std::io::Error>())
        .and_then(|io| classify_io_kind(io.kind()))
    {
        return class;
    }

    // --- Fallback: string-based classification ---
    let msg = format!("{:#}", err).to_lowercase();

    // Oracle: the source lifts `oracledb::Error` (Debug-only) into a string, so no downcast.
    if is_oracle_lost_session(&msg) {
        return TRANSIENT_RECONNECT;
    }

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
        // 53300's OTHER renderings — the max_connections cap says "sorry, too
        // many clients already" / "remaining connection slots are reserved";
        // "too many connections" is only the per-role/per-DB CONNECTION LIMIT
        // arm. Same gap, same fix as the doctor's categorizer.
        || msg.contains("too many clients")
        || msg.contains("connection slots are reserved")
        || msg.contains("the database system is starting up")
        || msg.contains("the database system is shutting down")
        || is_oracle_capacity(&msg)
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

/// The opendal error one link of an error chain is, or the one an `io::Error` from opendal's std writer wraps.
pub(crate) fn opendal_error<'a>(
    e: &'a (dyn std::error::Error + 'static),
) -> Option<&'a opendal::Error> {
    e.downcast_ref::<opendal::Error>().or_else(|| {
        e.downcast_ref::<std::io::Error>()?
            .get_ref()?
            .downcast_ref::<opendal::Error>()
    })
}

/// opendal's verdict on a store answer: a temporary one is a blip the destination side rides out, any other is final.
fn classify_opendal(e: &opendal::Error) -> RetryClass {
    if e.is_temporary() {
        transient(false, 500)
    } else {
        PERMANENT
    }
}

/// Classify a Postgres SQLSTATE code.
/// Reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>
fn classify_pg_sqlstate(code: &postgres::error::SqlState) -> RetryClass {
    use postgres::error::SqlState;

    // Class 08 — Connection Exception (08000, 08003, 08006, 08001, 08004, …) → reconnect
    if code.code().starts_with("08") {
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

/// A lost or killed Oracle session: the thin driver's dead-connection wording or an ORA code
/// for a killed session, a dropped channel, or an unreachable listener.
/// Oracle refused a session for capacity: a per-user or server session/process cap,
/// or a listener with no free handler. Clears when other sessions end.
fn is_oracle_capacity(msg: &str) -> bool {
    const CODES: &[&str] = &[
        "ora-02391", // exceeded simultaneous SESSIONS_PER_USER limit
        "ora-00018", // maximum number of sessions exceeded
        "ora-00020", // maximum number of processes exceeded
        "ora-12516", // TNS:listener could not find available handler
        "ora-12519", // TNS:no appropriate service handler found
        "ora-12520", // TNS:listener could not find available handler for requested type
    ];
    CODES.iter().any(|c| msg.contains(c))
}

/// Is `err` the source refusing to convert a value to a column's type (22P02 and kin, SQL Server 8114/245/241/8115, ORA-01722 and kin)?
pub(crate) fn is_value_conversion(err: &anyhow::Error) -> bool {
    use postgres::error::SqlState;
    let pg = err
        .chain()
        .find_map(|e| e.downcast_ref::<postgres::Error>())
        .and_then(postgres::Error::as_db_error);
    if let Some(db) = pg {
        return [
            SqlState::INVALID_TEXT_REPRESENTATION,
            SqlState::INVALID_DATETIME_FORMAT,
            SqlState::DATETIME_FIELD_OVERFLOW,
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ]
        .contains(db.code());
    }
    let msg = format!("{err:#}").to_lowercase();
    const CODES: &[&str] = &[
        "(code: 8114,", // error converting data type
        "(code: 245,",  // conversion failed converting a value
        "(code: 241,",  // conversion failed converting date/time
        "(code: 8115,", // arithmetic overflow converting
        "ora-01722",    // invalid number
        "ora-01858",    // a non-numeric character where a numeric was expected
        "ora-01861",    // literal does not match format string
        "ora-01841",    // year out of range
    ];
    CODES.iter().any(|c| msg.contains(c))
}

fn is_oracle_lost_session(msg: &str) -> bool {
    const CODES: &[&str] = &[
        "ora-00028", // your session has been killed
        "ora-03113", // end-of-file on communication channel
        "ora-03114", // not connected to ORACLE
        "ora-03135", // connection lost contact
        "ora-12537", // TNS:connection closed
        "ora-12170", // TNS:connect timeout occurred
        "ora-12541", // TNS:no listener
    ];
    msg.contains("the database or network closed the connection")
        || CODES.iter().any(|c| msg.contains(c))
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

/// Is this error worth another attempt at all?
///
/// The one question the CHUNK layer needs answered. It used to be test-only,
/// which is why the chunk-level re-claim had no way to ask it and re-dispatched
/// a deterministically failing chunk to its attempt cap — 300 minutes of
/// production query time on a 300 s statement timeout, measured in the field.
pub(crate) fn is_transient(err: &anyhow::Error) -> bool {
    classify_error(err).is_transient()
}

#[cfg(test)]
mod tests {
    /// Round-10 mutants: the `||` chain of transient needles was un-graded —
    /// each arm gets its own input so `&&` mutants die per-arm.
    /// An `io::Error` decides by its kind whatever its text says: a dropped connection with a
    /// bland message retries, a missing file whose message names a network word does not, and a
    /// kind that proves nothing leaves it to the text.
    #[test]
    fn an_io_error_kind_decides_before_its_text() {
        use std::io::{Error, ErrorKind};
        let class = |kind, text: &str| {
            classify_error(&anyhow::Error::new(Error::new(kind, text.to_string())))
        };
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionRefused,
            ErrorKind::NotConnected,
            ErrorKind::BrokenPipe,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
            ErrorKind::Interrupted,
            ErrorKind::WouldBlock,
        ] {
            assert!(class(kind, "x").is_transient(), "{kind:?}");
        }
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::NotADirectory,
        ] {
            assert!(
                !class(kind, "dns connection reset").is_transient(),
                "{kind:?}"
            );
        }
        assert!(class(ErrorKind::Other, "connection reset by peer").is_transient());
        assert!(!class(ErrorKind::Other, "x").is_transient());
    }

    #[test]
    fn each_transient_needle_arm_classifies_on_its_own() {
        for msg in [
            "FATAL: sorry, too many clients already",
            "FATAL: remaining connection slots are reserved for roles",
        ] {
            let c = classify_error(&anyhow::anyhow!("{msg}"));
            assert!(
                matches!(c, RetryClass::Transient { .. }),
                "{msg} must classify transient, got {c:?}"
            );
        }
    }

    /// A lost or killed Oracle session reconnects and retries, in the form the source renders it.
    #[test]
    fn a_lost_oracle_session_is_transient_and_reconnects() {
        for msg in [
            "oracle: the database or network closed the connection",
            "oracle: ORA-00028: your session has been killed",
            "oracle: ORA-03113: end-of-file on communication channel",
            "oracle: ORA-03114: not connected to ORACLE",
            "oracle: ORA-03135: connection lost contact",
            "oracle: ORA-12537: TNS:connection closed",
            "oracle: ORA-12170: TNS:Connect timeout occurred",
            "oracle: ORA-12541: TNS:no listener",
        ] {
            let c = classify_error(&anyhow::anyhow!("{msg}"));
            assert_eq!(c, TRANSIENT_RECONNECT, "{msg}");
        }
    }

    /// A session or process cap on Oracle clears as other sessions end: retry with the long delay.
    #[test]
    fn an_oracle_session_cap_is_transient_with_the_capacity_delay() {
        for msg in [
            "oracle: ORA-02391: exceeded simultaneous SESSIONS_PER_USER limit",
            "oracle: ORA-00018: maximum number of sessions exceeded",
            "oracle: ORA-00020: maximum number of processes (150) exceeded",
            "oracle: ORA-12516: TNS:listener could not find available handler",
            "oracle: ORA-12519: TNS:no appropriate service handler found",
            "oracle: ORA-12520: TNS:listener could not find available handler",
        ] {
            assert_eq!(
                classify_error(&anyhow::anyhow!("{msg}")),
                transient(true, 15_000),
                "{msg}"
            );
        }
    }

    /// Missing objects, bad credentials and syntax errors stay permanent on Oracle.
    #[test]
    fn a_permanent_oracle_error_is_not_retried() {
        for msg in [
            "oracle: ORA-00942: table or view \"RIVET\".\"NOPE\" does not exist",
            "oracle: ORA-00904: \"NOPE\": invalid identifier",
            "oracle: ORA-01017: invalid credential or not authorized; logon denied",
            "oracle: ORA-00900: invalid SQL statement",
            "oracle: ORA-00923: FROM keyword not found where expected",
            "oracle: ORA-00933: SQL command not properly ended",
        ] {
            let c = classify_error(&anyhow::anyhow!("{msg}"));
            assert_eq!(c, PERMANENT, "{msg}");
        }
    }

    /// The rule all three runners now share. Before it was one function it
    /// was five hand-written copies, none of them tested.
    #[test]
    fn a_connect_retry_needs_both_budget_and_a_recoverable_error() {
        assert!(
            should_retry(Attempt {
                attempt: 0,
                max_retries: 3,
                error: &transient_err()
            }),
            "budget left and a recoverable error is the whole retry case"
        );
        assert!(
            !should_retry(Attempt {
                attempt: 3,
                max_retries: 3,
                error: &transient_err()
            }),
            "attempt is zero-based, so attempt == max_retries is the LAST permitted attempt — \
             retrying from it runs max_retries + 1 times"
        );
        assert!(
            !should_retry(Attempt {
                attempt: 2,
                max_retries: 3,
                error: &permanent_err()
            }),
            "an auth failure is retried into the same wall; the operator waits out the full \
             backoff for an answer the first attempt already had"
        );
        assert!(
            !should_retry(Attempt {
                attempt: 0,
                max_retries: 0,
                error: &transient_err()
            }),
            "max_retries: 0 means exactly one attempt, recoverable or not"
        );
    }

    fn transient_err() -> anyhow::Error {
        anyhow::anyhow!("connection reset by peer")
    }

    fn permanent_err() -> anyhow::Error {
        anyhow::anyhow!("permission denied for table users")
    }

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

        // #field-3024: the MySQL variant is likewise a PERMANENT typed marker (a
        // retry just re-times-out), and its Display carries the actionable fix —
        // including the chunk_size_memory_mb hint for the WIDE `*_version` tables
        // whose raw driver 3024 gave field operators no guidance.
        let my: anyhow::Error = StatementDurationTimeout::mysql(300).into();
        assert_eq!(
            classify_error(&my),
            PERMANENT,
            "the mysql duration-timeout marker must be permanent too"
        );
        let mymsg = format!("{}", StatementDurationTimeout::mysql(300));
        assert!(
            mymsg.contains("mode: chunked")
                && mymsg.contains("chunk_size_memory_mb")
                && mymsg.contains("statement_timeout_s"),
            "the mysql Display must name the wide-table escape too: {mymsg}"
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

    #[test]
    fn a_typed_integrity_stop_is_never_retried_even_if_its_text_says_timeout() {
        let err: anyhow::Error =
            crate::error::DataIntegrityError::new(crate::quality::failure_message(
                "session_timeouts",
                &["column 'timeout_ms': 3 duplicate values"],
            ))
            .into();
        assert!(!classify_error(&err).is_transient());
        assert!(!should_retry(Attempt {
            attempt: 0,
            max_retries: 3,
            error: &err,
        }));
    }

    #[test]
    fn a_typed_drift_stop_is_never_retried_even_if_its_text_says_dns() {
        let err: anyhow::Error = crate::error::SchemaDriftError::new(
            "schema drift detected for export 'dns_records': column added",
        )
        .into();
        assert!(!classify_error(&err).is_transient());
    }

    #[test]
    fn a_coded_refusal_is_permanent_but_a_coded_environment_error_still_reads_its_text() {
        let refused: anyhow::Error = crate::error::CodedError::new(
            crate::error::codes::STATE_CURSOR_OWNER_MISMATCH,
            "connection reset while the cursor is owned elsewhere",
        )
        .into();
        assert!(!classify_error(&refused).is_transient());
        let env: anyhow::Error = crate::error::CodedError::new(
            crate::error::codes::SOURCE_STATEMENT_TIMEOUT,
            "connection reset by peer",
        )
        .into();
        assert!(classify_error(&env).is_transient());
    }

    /// Each credential needle classifies PERMANENT on its own, even beside a transient one.
    #[test]
    fn each_credential_needle_is_permanent_beside_a_transient_one() {
        for needle in [
            "loading credential",
            "loadcredential",
            "metadata.google.internal",
            "permission denied",
            "access denied",
            "invalid_grant",
            "token has been expired or revoked",
        ] {
            let c = classify_error(&anyhow::anyhow!("{needle}: connection reset by peer"));
            assert_eq!(c, PERMANENT, "{needle}");
        }
    }

    /// Each OpenDAL transient needle classifies as a brief destination blip on its own.
    #[test]
    fn each_destination_blip_needle_classifies_on_its_own() {
        for msg in [
            "Unexpected (temporary) at write",
            "dispatch task is gone",
            "runtime dropped the dispatch task",
            "send http request, source: client error (SendRequest)",
        ] {
            assert_eq!(
                classify_error(&anyhow::anyhow!("{msg}")),
                transient(false, 500),
                "{msg}"
            );
        }
        // The SendRequest pair counts only together.
        for half in ["client error (SendRequest)", "send http request"] {
            assert_ne!(
                classify_error(&anyhow::anyhow!("{half}")),
                transient(false, 500),
                "{half}"
            );
        }
    }

    /// Each server-shutdown SQLSTATE reconnects after the long delay, on its own.
    #[test]
    fn each_pg_shutdown_sqlstate_reconnects_after_a_delay() {
        use postgres::error::SqlState;
        for code in [
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
        ] {
            assert_eq!(
                classify_pg_sqlstate(&code),
                transient(true, 15_000),
                "{code:?}"
            );
        }
    }

    /// Each MySQL server code maps to its class by code, whatever the message says.
    #[test]
    fn each_mysql_code_maps_to_its_class() {
        let class = |code: u16| {
            classify_mysql_error(&mysql::Error::MySqlError(mysql::error::MySqlError {
                state: "HY000".to_string(),
                message: "x".to_string(),
                code,
            }))
        };
        assert_eq!(class(1213), Some(transient(false, 1_000)));
        assert_eq!(class(1205), Some(TRANSIENT_SAME_CONN));
        for code in [1040, 1053] {
            assert_eq!(class(code), Some(transient(true, 15_000)), "{code}");
        }
        for code in [1045, 1044, 1049, 1146, 1064, 3024] {
            assert_eq!(class(code), Some(PERMANENT), "{code}");
        }
        assert_eq!(class(9999), None);
    }

    /// Each string needle of the network, duration and lock-wait groups classifies on its own.
    #[test]
    fn each_string_needle_classifies_on_its_own() {
        for (needles, want) in [
            (
                &[
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
                    "gone away",
                    "lost connection",
                    "the server closed the connection",
                    "can't connect to mysql server",
                ][..],
                TRANSIENT_RECONNECT,
            ),
            (
                &[
                    "statement timeout after",
                    "due to statement timeout",
                    "execution time exceeded",
                    "max_execution_time",
                ][..],
                PERMANENT,
            ),
            (
                &[
                    "timed out",
                    "timeout",
                    "canceling statement",
                    "lock wait timeout",
                ][..],
                TRANSIENT_SAME_CONN,
            ),
        ] {
            for needle in needles {
                assert_eq!(
                    classify_error(&anyhow::anyhow!("{needle}")),
                    want,
                    "{needle}"
                );
            }
        }
        // A duration needle wins over the lock-wait needle that follows it in the text.
        for needle in [
            "statement timeout after",
            "due to statement timeout",
            "execution time exceeded",
            "max_execution_time",
        ] {
            let c = classify_error(&anyhow::anyhow!("{needle}; the query timed out"));
            assert_eq!(c, PERMANENT, "{needle}");
        }
    }

    /// Only 57014 is the duration cancel; a resource SQLSTATE past it keeps its own class.
    #[test]
    fn only_query_canceled_is_permanent_among_late_pg_states() {
        use postgres::error::SqlState;
        assert_eq!(classify_pg_sqlstate(&SqlState::QUERY_CANCELED), PERMANENT);
        assert_eq!(
            classify_pg_sqlstate(&SqlState::DISK_FULL),
            transient(false, 5_000)
        );
        for code in [SqlState::CONNECTION_EXCEPTION, SqlState::CONNECTION_FAILURE] {
            assert_eq!(classify_pg_sqlstate(&code), TRANSIENT_RECONNECT, "{code:?}");
        }
    }

    /// A value-conversion refusal is recognised by its engine code, and nothing else is.
    #[test]
    fn a_value_conversion_is_recognised_by_its_engine_code() {
        for msg in [
            "mssql: streaming rows failed: Token error: 'Error converting data type varchar to bigint.' on server s executing  on line 1 (code: 8114, state: 5, class: 16)",
            "Token error: 'Conversion failed when converting the varchar value 'x' to data type int.' (code: 245, state: 1, class: 16)",
            "Token error: 'Conversion failed when converting date and/or time from character string.' (code: 241, state: 1, class: 16)",
            "Token error: 'Arithmetic overflow error converting varchar to data type numeric.' (code: 8115, state: 8, class: 16)",
            "ORA-01722: invalid number",
            "ORA-01858: a non-numeric character was found where a numeric was expected",
            "ORA-01861: literal does not match format string",
            "ORA-01841: (full) year must be between -4713 and +9999",
        ] {
            assert!(is_value_conversion(&anyhow::anyhow!("{msg}")), "{msg}");
        }
        for msg in [
            "Token error: 'Invalid object name.' (code: 208, state: 1, class: 16)",
            "Token error: 'x' (code: 82450, state: 1, class: 16)",
            "ORA-00942: table or view does not exist",
            "connection reset by peer",
        ] {
            assert!(!is_value_conversion(&anyhow::anyhow!("{msg}")), "{msg}");
        }
    }

    /// A store's final answer is classified by opendal's verdict, not by words in its header dump: Azurite's 404 carries `keep-alive: timeout=5`.
    #[test]
    fn an_object_store_answer_is_classified_by_opendal_s_verdict_not_its_text() {
        let azurite_404 = || {
            opendal::Error::new(
                opendal::ErrorKind::NotFound,
                "AzblobError { code: \"ContainerNotFound\" }",
            )
            .with_context("response", "headers: {\"keep-alive\": \"timeout=5\"}")
        };
        let raw = anyhow::Error::new(azurite_404()).context("upload");
        assert!(format!("{raw:#}").contains("timeout"), "{raw:#}");
        assert_eq!(classify_error(&raw), PERMANENT, "{raw:#}");
        // The streamed writer hands the same answer back inside an `io::Error`.
        let streamed = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            azurite_404(),
        ));
        assert_eq!(classify_error(&streamed), PERMANENT, "{streamed:#}");
        let denied = anyhow::Error::new(opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            "request timeout policy",
        ));
        assert_eq!(classify_error(&denied), PERMANENT);
        let blip = anyhow::Error::new(
            opendal::Error::new(opendal::ErrorKind::Unexpected, "send http request")
                .set_temporary(),
        );
        assert_eq!(classify_error(&blip), transient(false, 500));
        assert!(
            classify_error(&anyhow::anyhow!("i/o timeout")).is_transient(),
            "text still decides for a non-store error"
        );
    }
}
