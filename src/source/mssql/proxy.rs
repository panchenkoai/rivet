//! SQL Server connection-proxy classifier — distinguishes a direct connection
//! from a transaction-mode multiplexer or an Azure SQL gateway in front of the
//! instance.
//!
//! The SQL Server analogue of [`crate::source::mysql::proxy`] and the Postgres
//! `detect_pg_transaction_pooler`: it runs once at connect time so the operator
//! gets a one-line warning when a pooler/gateway sits in front of the database
//! (session `SET` options, `#temp` tables, and any open cursor may not survive
//! statement-level multiplexing). [`classify_mssql_proxy`] is a pure function,
//! exhaustively unit-tested in this file; the I/O wrapper
//! [`detect_mssql_proxy_kind`] collects the live signals and delegates.

use tokio::runtime::Runtime;

use super::{MssqlClient, scalar_to_string};

/// What the SQL Server connection is actually talking to.
///
/// Detection happens once at connect time via [`detect_mssql_proxy_kind`].
///
/// `pub` for integration-test reachability via `MssqlSource::proxy_kind()`;
/// same "no external API contract" disclaimer applies as for the rest of
/// `rivet::source::mssql::*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MssqlProxyKind {
    /// Direct connection to a SQL Server instance — no proxy detected.
    Direct,
    /// A transaction-mode multiplexer / connection router: detected because
    /// `@@SPID` returned different session ids across two consecutive queries
    /// on the same connection. The proxy is handing each statement to a
    /// different backend session, so `SET` options, `#temp` tables, and cursors
    /// will not persist across statements.
    ///
    /// False negatives are possible when the proxy's backend pool size is 1
    /// (the same physical session is always reused).
    Multiplexed,
    /// Azure SQL Database / Managed Instance: detected via
    /// `SERVERPROPERTY('EngineEdition')` of `5` (Azure SQL DB) or `8` (Managed
    /// Instance), falling back to the `@@VERSION` banner. These instances are
    /// always fronted by the Azure SQL gateway — the session is preserved, but
    /// the gateway may proxy *or* redirect the connection, enforces idle
    /// timeouts, and injects transient faults.
    AzureGateway,
}

impl MssqlProxyKind {
    /// True for any non-direct connection (`is_proxy() == false` only for
    /// [`MssqlProxyKind::Direct`]).
    ///
    /// `#[allow(dead_code)]` because the binary compilation unit (which
    /// re-declares `mod source`) does not reference this; the lib + tests do.
    #[allow(dead_code)]
    pub fn is_proxy(self) -> bool {
        !matches!(self, MssqlProxyKind::Direct)
    }

    /// Stable label for diagnostic logs. Keep terse and stable: external log
    /// parsers grep on these strings.
    #[allow(dead_code)]
    pub fn log_label(self) -> &'static str {
        match self {
            MssqlProxyKind::Direct => "direct",
            MssqlProxyKind::Multiplexed => "mssql-multiplexed",
            MssqlProxyKind::AzureGateway => "azure-gateway",
        }
    }

    /// One-time warning emitted at connect time. Returns `None` for
    /// [`MssqlProxyKind::Direct`] (the common case, no warning needed).
    fn warn_message(self) -> Option<&'static str> {
        match self {
            MssqlProxyKind::Direct => None,
            MssqlProxyKind::Multiplexed => Some(
                "SQL Server connection multiplexing detected (@@SPID differs across queries) \
                 — session SET options, #temp tables, and cursors may not persist across \
                 statements; use a direct (non-pooled) connection for production exports",
            ),
            MssqlProxyKind::AzureGateway => Some(
                "Azure SQL gateway detected (EngineEdition Azure) — the connection may be \
                 proxied or redirected and is subject to gateway idle timeouts and transient \
                 faults; keep retries enabled and prefer the Redirect connection policy for \
                 throughput",
            ),
        }
    }
}

/// Pure classifier for SQL Server proxy detection signals. Kept separate from
/// [`detect_mssql_proxy_kind`] so it can be exhaustively unit-tested without a
/// live SQL Server. See [`MssqlProxyKind`] for the meaning of each variant.
///
/// Precedence is intentional:
///
/// 1. `@@SPID` differing across two queries → [`MssqlProxyKind::Multiplexed`].
///    This is the strongest *risk* signal: a true statement-level multiplexer
///    breaks session state even on Azure, so it wins over the edition probe.
/// 2. `SERVERPROPERTY('EngineEdition')` of `5`/`8`, or an Azure `@@VERSION`
///    banner → [`MssqlProxyKind::AzureGateway`].
/// 3. Otherwise → [`MssqlProxyKind::Direct`].
fn classify_mssql_proxy(
    spid_pair: Option<(i32, i32)>,
    engine_edition: Option<i32>,
    version_banner: Option<&str>,
) -> MssqlProxyKind {
    if let Some((a, b)) = spid_pair
        && a != b
    {
        return MssqlProxyKind::Multiplexed;
    }
    if matches!(engine_edition, Some(5) | Some(8)) {
        return MssqlProxyKind::AzureGateway;
    }
    if let Some(v) = version_banner {
        let l = v.to_ascii_lowercase();
        if l.contains("sql azure") || l.contains("azure sql") {
            return MssqlProxyKind::AzureGateway;
        }
    }
    MssqlProxyKind::Direct
}

/// I/O wrapper around [`classify_mssql_proxy`]: collects the detection signals
/// from a live connection and returns the classification. On any query failure
/// the missing signal is simply dropped — detection is best-effort and must
/// never break a real export (worst case it reports [`MssqlProxyKind::Direct`]).
pub(super) fn detect_mssql_proxy_kind(rt: &Runtime, client: &mut MssqlClient) -> MssqlProxyKind {
    rt.block_on(async {
        // `@@SPID` is the session id; comparing two consecutive calls on the
        // same connection detects transaction-mode multiplexers that hand each
        // statement to a different backend session. `CAST(... AS INT)` so the
        // value decodes uniformly (raw `@@SPID` is smallint).
        let spid1 = scalar_i32(client, "SELECT CAST(@@SPID AS INT)").await;
        let spid2 = scalar_i32(client, "SELECT CAST(@@SPID AS INT)").await;
        let edition = scalar_i32(
            client,
            "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)",
        )
        .await;
        let banner = scalar_string(client, "SELECT @@VERSION").await;
        let pair = match (spid1, spid2) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        };
        classify_mssql_proxy(pair, edition, banner.as_deref())
    })
}

/// Fetch a single `INT` scalar; `None` on any driver error or NULL.
async fn scalar_i32(client: &mut MssqlClient, sql: &str) -> Option<i32> {
    let row = client.query(sql, &[]).await.ok()?.into_row().await.ok()??;
    row.get::<i32, _>(0)
}

/// Fetch a single string scalar via the shared [`scalar_to_string`] decoder;
/// `None` on any driver error or NULL.
async fn scalar_string(client: &mut MssqlClient, sql: &str) -> Option<String> {
    let row = client.query(sql, &[]).await.ok()?.into_row().await.ok()??;
    scalar_to_string(&row)
}

/// Emit the one-time connect-time warning for a non-direct proxy kind.
/// Centralized so the wording stays consistent across connect entry points.
pub(super) fn warn_proxy_kind(kind: MssqlProxyKind) {
    if let Some(msg) = kind.warn_message() {
        log::warn!("{msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::{MssqlProxyKind, classify_mssql_proxy};

    #[test]
    fn classify_direct_when_no_signals() {
        assert_eq!(
            classify_mssql_proxy(None, None, None),
            MssqlProxyKind::Direct
        );
    }

    #[test]
    fn classify_direct_when_spids_match_and_on_prem_edition() {
        // EngineEdition 3 = Enterprise/Developer (the dev container).
        assert_eq!(
            classify_mssql_proxy(Some((53, 53)), Some(3), Some("Microsoft SQL Server 2022")),
            MssqlProxyKind::Direct
        );
    }

    #[test]
    fn classify_multiplexed_when_spid_drifts() {
        assert_eq!(
            classify_mssql_proxy(Some((53, 71)), Some(3), None),
            MssqlProxyKind::Multiplexed
        );
    }

    #[test]
    fn classify_azure_via_engine_edition_5() {
        assert_eq!(
            classify_mssql_proxy(Some((100, 100)), Some(5), None),
            MssqlProxyKind::AzureGateway
        );
    }

    #[test]
    fn classify_azure_via_managed_instance_edition_8() {
        assert_eq!(
            classify_mssql_proxy(None, Some(8), None),
            MssqlProxyKind::AzureGateway
        );
    }

    #[test]
    fn classify_azure_via_version_banner_when_edition_missing() {
        assert_eq!(
            classify_mssql_proxy(
                Some((9, 9)),
                None,
                Some("Microsoft SQL Azure (RTM) - 12.0.2000.8")
            ),
            MssqlProxyKind::AzureGateway
        );
    }

    #[test]
    fn classify_multiplexed_takes_precedence_over_azure() {
        // A real statement-level multiplexer breaks session state even on
        // Azure, so SPID drift must win over the edition probe.
        assert_eq!(
            classify_mssql_proxy(Some((1, 2)), Some(5), Some("Microsoft SQL Azure")),
            MssqlProxyKind::Multiplexed
        );
    }

    #[test]
    fn classify_direct_when_spid_pair_missing_and_on_prem() {
        assert_eq!(
            classify_mssql_proxy(None, Some(2), Some("Microsoft SQL Server 2019")),
            MssqlProxyKind::Direct
        );
    }

    #[test]
    fn classify_azure_banner_case_insensitive() {
        assert_eq!(
            classify_mssql_proxy(None, None, Some("...AZURE SQL...")),
            MssqlProxyKind::AzureGateway
        );
    }

    // ── Warning / label contract ────────────────────────────────────────

    #[test]
    fn is_proxy_helper_matches_variants() {
        assert!(!MssqlProxyKind::Direct.is_proxy());
        assert!(MssqlProxyKind::Multiplexed.is_proxy());
        assert!(MssqlProxyKind::AzureGateway.is_proxy());
    }

    #[test]
    fn direct_has_no_warning() {
        assert!(MssqlProxyKind::Direct.warn_message().is_none());
    }

    #[test]
    fn non_direct_variants_have_warnings() {
        for k in [MssqlProxyKind::Multiplexed, MssqlProxyKind::AzureGateway] {
            assert!(
                k.warn_message().is_some(),
                "{k:?} must emit a warning at connect time"
            );
        }
    }

    #[test]
    fn log_labels_are_stable_and_distinct() {
        let labels = [
            MssqlProxyKind::Direct.log_label(),
            MssqlProxyKind::Multiplexed.log_label(),
            MssqlProxyKind::AzureGateway.log_label(),
        ];
        assert_eq!(labels, ["direct", "mssql-multiplexed", "azure-gateway"]);
    }
}
