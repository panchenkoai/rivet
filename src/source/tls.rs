//! Transport-security setup shared by Postgres, MySQL, and preflight paths.
//!
//! Both drivers eventually need a `native_tls::TlsConnector`, then wrap it in a
//! driver-specific adapter (`postgres_native_tls::MakeTlsConnector` for
//! `postgres::Client::connect`, and `mysql::SslOpts` for `mysql::Pool`).
//!
//! The single entry point [`build_native_tls`] translates the user-facing
//! [`TlsConfig`] (mode + optional CA file + danger-knobs) into a configured
//! `native_tls::TlsConnector`. Error messages deliberately avoid leaking the
//! configured CA path into the anyhow chain that ends up in
//! `summary.error_message` / Slack — we surface only the failure reason.

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;

/// Build a `native_tls::TlsConnector` from a [`TlsConfig`].
///
/// Caller is responsible for branching on `cfg.mode == TlsMode::Disable` and
/// using plaintext (`NoTls` / no `ssl_opts`) in that path — this function
/// assumes TLS is enforced.
pub(crate) fn build_native_tls(cfg: &TlsConfig) -> Result<native_tls::TlsConnector> {
    debug_assert!(
        cfg.mode.is_enforced(),
        "build_native_tls called with Disable mode"
    );

    let mut builder = native_tls::TlsConnector::builder();

    match cfg.mode {
        TlsMode::Require => {
            // Protect against passive sniffing; skip chain + hostname checks.
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyCa => {
            // Trust chain is verified; hostname is not.
            builder.danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyFull => {
            // Full verification (chain + hostname). This is the secure default.
        }
        TlsMode::Disable => unreachable!(),
    }

    if let Some(path) = &cfg.ca_file {
        let pem =
            std::fs::read(path).map_err(|e| anyhow::anyhow!("tls: cannot read ca_file: {}", e))?;
        let cert = native_tls::Certificate::from_pem(&pem)
            .map_err(|e| anyhow::anyhow!("tls: ca_file is not valid PEM: {}", e))?;
        builder.add_root_certificate(cert);
    }

    // Honor the explicit danger knobs on top of the mode-derived defaults so
    // that, for example, `mode: verify-full` + `accept_invalid_hostnames: true`
    // works for IP-addressed hosts behind a cert. Each one emits a warning at
    // config-time (see `Config::validate`).
    if cfg.accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    if cfg.accept_invalid_hostnames {
        builder.danger_accept_invalid_hostnames(true);
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("tls: could not initialize native-tls connector: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mode: TlsMode) -> TlsConfig {
        TlsConfig {
            mode,
            ca_file: None,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        }
    }

    #[test]
    fn builder_succeeds_for_enforced_modes() {
        assert!(build_native_tls(&cfg(TlsMode::Require)).is_ok());
        assert!(build_native_tls(&cfg(TlsMode::VerifyCa)).is_ok());
        assert!(build_native_tls(&cfg(TlsMode::VerifyFull)).is_ok());
    }

    #[test]
    fn missing_ca_file_surfaces_clear_error() {
        let c = TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: Some("/nonexistent/rivet-test-ca.pem".into()),
            ..Default::default()
        };
        let err = build_native_tls(&c).unwrap_err();
        assert!(err.to_string().contains("cannot read ca_file"), "{err}");
    }

    #[test]
    fn invalid_pem_is_rejected() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"not a pem cert").unwrap();
        let c = TlsConfig {
            mode: TlsMode::VerifyFull,
            ca_file: Some(tmp.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let err = build_native_tls(&c).unwrap_err();
        assert!(err.to_string().contains("not valid PEM"), "{err}");
    }
}
