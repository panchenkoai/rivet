//! MySQL connection-proxy classifier — distinguishes direct connections from
//! ProxySQL / MaxScale / generic multiplexers.
//!
//! Runs once at connect time so the operator gets a one-line warning when a
//! proxy is in front of the database (session vars and temporary state may
//! not survive multiplexing).  See [`classify_mysql_proxy`] for the
//! detection precedence; it is a pure function and exhaustively
//! unit-tested in this file.  The I/O wrapper [`detect_mysql_proxy_kind`]
//! collects the live signals and delegates.

use mysql::Pool;
use mysql::prelude::*;

/// What the MySQL connection is actually talking to.
///
/// Used to:
/// - decide which warning (if any) to print at connect time,
/// - tag the `executing query (connection=...)` debug log so operators can
///   distinguish direct vs proxied traffic when reading logs after the fact.
///
/// Detection happens once at connect time via [`detect_mysql_proxy_kind`].
///
/// `pub` for integration-test reachability via `MysqlSource::proxy_kind()`;
/// same "no external API contract" disclaimer applies as for the rest of
/// `rivet::source::mysql::*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlProxyKind {
    /// Direct connection to a MySQL server — no proxy detected.
    Direct,
    /// ProxySQL: detected via either the `@@version_comment` signature or
    /// `@@proxy_version` (a ProxySQL-only system variable).
    ProxySql,
    /// MariaDB MaxScale: detected via `@@version_comment` containing
    /// "maxscale".  MaxScale's `readwritesplit` and `readconnroute` routers
    /// do *not* multiplex by default but they can still rewrite or block
    /// queries — worth surfacing to the operator.
    MaxScale,
    /// An unknown transaction-mode multiplexer: detected because
    /// `CONNECTION_ID()` returned different values across two consecutive
    /// queries on the same `mysql::Conn`.  This catches in-house balancers,
    /// HAProxy-with-MySQL-mode setups, and ProxySQL/MaxScale instances that
    /// hide their banner.
    ///
    /// False negatives are possible when the proxy's backend pool_size is 1
    /// (the same physical backend is always reused).
    Multiplexed,
}

impl MysqlProxyKind {
    /// True for any non-direct connection (`is_proxy() == false` only for
    /// [`MysqlProxyKind::Direct`]).
    ///
    /// `#[allow(dead_code)]` because the binary compilation unit (which
    /// re-declares `mod source`) does not reference this; the lib + tests do.
    /// Same pattern as `MysqlSource::from_pool`.
    #[allow(dead_code)]
    pub fn is_proxy(self) -> bool {
        !matches!(self, MysqlProxyKind::Direct)
    }

    /// Stable label for the `executing query (connection=...)` debug log.
    /// Keep this terse and stable: external log parsers grep on these strings.
    pub fn log_label(self) -> &'static str {
        match self {
            MysqlProxyKind::Direct => "direct",
            MysqlProxyKind::ProxySql => "proxysql",
            MysqlProxyKind::MaxScale => "maxscale",
            MysqlProxyKind::Multiplexed => "proxy-multiplexed",
        }
    }

    /// One-time warning emitted at connect time.  Returns `None` for
    /// [`MysqlProxyKind::Direct`] (the common case, no warning needed).
    fn warn_message(self) -> Option<&'static str> {
        match self {
            MysqlProxyKind::Direct => None,
            MysqlProxyKind::ProxySql => Some(
                "MySQL proxy multiplexer detected (ProxySQL) — session variables \
                 set per-connection may not survive multiplexing; use direct connections \
                 for production exports",
            ),
            MysqlProxyKind::MaxScale => Some(
                "MySQL proxy detected (MaxScale) — queries may be rewritten or routed; \
                 verify SQL is accepted by the active MaxScale router (readwritesplit, \
                 readconnroute) and that session timeouts apply on the backend",
            ),
            MysqlProxyKind::Multiplexed => Some(
                "MySQL connection multiplexing detected (CONNECTION_ID() differs across \
                 queries) — session variables and temporary state may not persist across \
                 statements; use direct connections for production exports",
            ),
        }
    }
}

/// Pure classifier for proxy detection signals.  Kept separate from
/// [`detect_mysql_proxy_kind`] so it can be exhaustively unit-tested without a
/// live MySQL.  See [`MysqlProxyKind`] for the meaning of each variant.
///
/// Precedence is intentional:
///
/// 1. `PROXYSQL INTERNAL SESSION` accepted as a query (ProxySQL intercepts
///    this command on its client port; vanilla MySQL returns a syntax
///    error). This is the strongest signal because ProxySQL by default
///    forwards `@@version_comment`, `VERSION()`, and `@@version` straight
///    through to the backend, so a configured-as-default ProxySQL is
///    invisible to banner checks.
/// 2. Explicit banner match in `@@version_comment` (ProxySQL > MaxScale).
///    Catches ProxySQL builds with `server_version` overridden to advertise
///    "ProxySQL" in `VERSION()`, and MaxScale which puts "MaxScale" in the
///    banner.
/// 3. `@@proxy_version` presence (ProxySQL-only system variable when
///    `mysql_query_rules` is configured to expose it).
/// 4. `CONNECTION_ID()` differing across two queries (generic multiplexing
///    fallback — catches HAProxy MySQL mode, custom balancers, etc.).
///
/// Banner-before-CONNECTION_ID order matters: a ProxySQL behind a
/// `transaction_persistent` user (which keeps the same backend conn) would
/// fool the CONNECTION_ID check, so the specific signal wins.
fn classify_mysql_proxy(
    proxysql_internal_accepted: bool,
    version_comment: Option<&str>,
    proxy_version: Option<&str>,
    connection_id_pair: Option<(u64, u64)>,
) -> MysqlProxyKind {
    if proxysql_internal_accepted {
        return MysqlProxyKind::ProxySql;
    }
    if let Some(v) = version_comment {
        let l = v.to_ascii_lowercase();
        if l.contains("proxysql") {
            return MysqlProxyKind::ProxySql;
        }
        if l.contains("maxscale") {
            return MysqlProxyKind::MaxScale;
        }
    }
    if proxy_version.is_some() {
        return MysqlProxyKind::ProxySql;
    }
    if let Some((a, b)) = connection_id_pair
        && a != b
    {
        return MysqlProxyKind::Multiplexed;
    }
    MysqlProxyKind::Direct
}

/// I/O wrapper around [`classify_mysql_proxy`]: collects the detection
/// signals from a live connection and returns the classification.  On any
/// connection failure returns [`MysqlProxyKind::Direct`] — detection is
/// best-effort and must never break a real export.
pub(super) fn detect_mysql_proxy_kind(pool: &Pool) -> MysqlProxyKind {
    let mut conn = match pool.get_conn() {
        Ok(c) => c,
        Err(_) => return MysqlProxyKind::Direct,
    };
    detect_proxy_on_conn(&mut conn)
}

/// The probe + classify on an already-open connection — shared by the batch pool
/// path and the CDC path (which holds a bare `Conn`, not a `Pool`). A proxy that
/// shows up here cannot carry the binlog/replication protocol CDC needs.
pub(super) fn detect_proxy_on_conn<C: Queryable>(conn: &mut C) -> MysqlProxyKind {
    // `PROXYSQL INTERNAL SESSION` is intercepted by ProxySQL on its client
    // port (6033) and returns a single-column JSON row describing the
    // proxy session; vanilla MySQL and MariaDB return SQL syntax error
    // 1064.  We use `query_drop` so we don't have to model the response
    // shape — any `Ok` indicates ProxySQL accepted the command.
    //
    // (`PROXYSQL VERSION` exists too but is only accepted on the admin
    // port 6032, not on the client port we connect through.)
    let proxysql_internal_accepted: bool = conn.query_drop("PROXYSQL INTERNAL SESSION").is_ok();
    let version_comment: Option<String> =
        conn.query_first("SELECT @@version_comment").unwrap_or(None);
    let proxy_version: Option<String> = conn.query_first("SELECT @@proxy_version").unwrap_or(None);
    // CONNECTION_ID() is server-side: comparing two consecutive calls on the
    // same `Conn` detects transaction-mode multiplexers that hand each
    // statement to a different backend connection.
    let cid1: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let cid2: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let pair = match (cid1, cid2) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    };
    classify_mysql_proxy(
        proxysql_internal_accepted,
        version_comment.as_deref(),
        proxy_version.as_deref(),
        pair,
    )
}

/// Emit the one-time connect-time warning for a non-direct proxy kind.
/// Centralized so the wording stays consistent across the three connect entry
/// points (`from_pool`, `connect`, `connect_with_tls`).
pub(super) fn warn_proxy_kind(kind: MysqlProxyKind) {
    if let Some(msg) = kind.warn_message() {
        log::warn!("{msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::{MysqlProxyKind, classify_mysql_proxy};

    #[test]
    fn proxy_classify_direct_when_no_signals() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_all_signals_missing() {
        let kind = classify_mysql_proxy(false, None, None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_via_internal_command() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((7, 7)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("(ProxySQL) High Performance MySQL Proxy"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner_lowercase() {
        let kind = classify_mysql_proxy(false, Some("(proxysql) hpmp"), None, None);
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_proxy_version_only() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            Some("2.5.5-percona-1.1"),
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_maxscale_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("MariaDB MaxScale 22.08.4-ge6a8d35ec source distribution"),
            None,
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::MaxScale);
    }

    #[test]
    fn proxy_classify_proxysql_internal_takes_precedence_over_banner_maxscale() {
        let kind = classify_mysql_proxy(
            true,
            Some("MariaDB MaxScale 22.08.4 source distribution"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_takes_precedence_over_maxscale_in_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("ProxySQL bridging MaxScale upstream"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_multiplexed_via_connection_id_drift() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server"),
            None,
            Some((100, 200)),
        );
        assert_eq!(kind, MysqlProxyKind::Multiplexed);
    }

    #[test]
    fn proxy_classify_direct_when_connection_id_pair_missing() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_connection_ids_match() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, Some((7, 7)));
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_pool_size_one_still_caught_by_banner() {
        let kind = classify_mysql_proxy(false, Some("(ProxySQL) HPMP"), None, Some((5, 5)));
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_default_config_still_detected_via_internal() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(
            kind,
            MysqlProxyKind::ProxySql,
            "default-config ProxySQL must be detectable via PROXYSQL INTERNAL signal alone"
        );
    }

    // ── Warning / label contract ────────────────────────────────────────

    #[test]
    fn proxy_kind_is_proxy_helper_matches_variants() {
        assert!(!MysqlProxyKind::Direct.is_proxy());
        assert!(MysqlProxyKind::ProxySql.is_proxy());
        assert!(MysqlProxyKind::MaxScale.is_proxy());
        assert!(MysqlProxyKind::Multiplexed.is_proxy());
    }

    #[test]
    fn proxy_kind_direct_has_no_warning() {
        assert!(MysqlProxyKind::Direct.warn_message().is_none());
    }

    #[test]
    fn proxy_kind_non_direct_variants_have_warnings() {
        for k in [
            MysqlProxyKind::ProxySql,
            MysqlProxyKind::MaxScale,
            MysqlProxyKind::Multiplexed,
        ] {
            assert!(
                k.warn_message().is_some(),
                "{k:?} must emit a warning at connect time"
            );
        }
    }

    #[test]
    fn proxy_kind_log_labels_are_stable_and_distinct() {
        let labels = [
            MysqlProxyKind::Direct.log_label(),
            MysqlProxyKind::ProxySql.log_label(),
            MysqlProxyKind::MaxScale.log_label(),
            MysqlProxyKind::Multiplexed.log_label(),
        ];
        assert_eq!(
            labels,
            ["direct", "proxysql", "maxscale", "proxy-multiplexed"]
        );
    }
}
