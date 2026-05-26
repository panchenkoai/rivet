//! SecOps tests — artifact credential redaction (ADR-0005 PA9) and TLS config.

use super::*;

// ─── ADR-0005 PA9 — artifact credential redaction ─────────────────────────

fn src_with_password(password: Option<&str>, url: Option<&str>) -> SourceConfig {
    SourceConfig {
        source_type: SourceType::Postgres,
        url: url.map(String::from),
        url_env: None,
        url_file: None,
        host: Some("db.example.com".into()),
        port: Some(5432),
        user: Some("rivet".into()),
        password: password.map(String::from),
        password_env: Some("DB_PASSWORD".into()),
        database: Some("prod".into()),
        environment: None,
        tuning: None,
        tls: None,
    }
}

#[test]
fn redact_plaintext_password_stripped() {
    let src = src_with_password(Some("s3cret!"), None);
    let (safe, redacted) = src.redact_for_artifact();
    assert!(
        redacted,
        "redaction must be reported for plaintext password"
    );
    assert_eq!(safe.password, None);
    // Non-secret references are preserved so apply can re-resolve at runtime.
    assert_eq!(safe.password_env.as_deref(), Some("DB_PASSWORD"));
    assert_eq!(safe.user.as_deref(), Some("rivet"));
    assert_eq!(safe.host.as_deref(), Some("db.example.com"));
}

#[test]
fn redact_password_embedded_in_url() {
    let src = src_with_password(
        None,
        Some("postgresql://rivet:s3cret@db.example.com:5432/prod"),
    );
    let (safe, redacted) = src.redact_for_artifact();
    assert!(redacted);
    let url = safe.url.unwrap();
    assert!(
        !url.contains("s3cret"),
        "plaintext password must not remain: {url}"
    );
    assert!(
        url.contains("REDACTED@"),
        "userinfo must be replaced: {url}"
    );
    assert!(
        url.contains("db.example.com:5432/prod"),
        "tail preserved: {url}"
    );
}

#[test]
fn redact_url_without_userinfo_is_unchanged() {
    let src = src_with_password(None, Some("postgresql://db.example.com:5432/prod"));
    let (safe, redacted) = src.redact_for_artifact();
    assert!(!redacted, "no secrets → no redaction flag");
    assert_eq!(
        safe.url.as_deref(),
        Some("postgresql://db.example.com:5432/prod")
    );
}

#[test]
fn redact_env_references_are_preserved() {
    let src = SourceConfig {
        source_type: SourceType::Postgres,
        url: None,
        url_env: Some("DATABASE_URL".into()),
        url_file: Some("/etc/secrets/db".into()),
        host: None,
        port: None,
        user: None,
        password: None,
        password_env: Some("DB_PWD".into()),
        database: None,
        environment: None,
        tuning: None,
        tls: None,
    };
    let (safe, redacted) = src.redact_for_artifact();
    assert!(!redacted, "env-only config has nothing to redact");
    assert_eq!(safe.url_env.as_deref(), Some("DATABASE_URL"));
    assert_eq!(safe.url_file.as_deref(), Some("/etc/secrets/db"));
    assert_eq!(safe.password_env.as_deref(), Some("DB_PWD"));
}

#[test]
fn redact_does_not_confuse_at_in_path() {
    // `@` in the path (after the first `/`) must not be treated as userinfo boundary.
    let src = src_with_password(None, Some("postgresql://db.example.com/prod?filter=a@b"));
    let (safe, redacted) = src.redact_for_artifact();
    assert!(!redacted);
    assert_eq!(
        safe.url.as_deref(),
        Some("postgresql://db.example.com/prod?filter=a@b")
    );
}

// ─── TLS block (SecOps) ─────────────────────────────────────────────────────

#[test]
fn tls_absent_deserializes_as_none() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert!(cfg.source.tls.is_none());
}

#[test]
fn tls_verify_full_with_ca_file_parses() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tls:
    mode: verify-full
    ca_file: /etc/ssl/certs/rds.pem
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let tls = cfg.source.tls.expect("tls block must parse");
    assert_eq!(tls.mode, TlsMode::VerifyFull);
    assert_eq!(tls.ca_file.as_deref(), Some("/etc/ssl/certs/rds.pem"));
    assert!(!tls.accept_invalid_certs);
    assert!(!tls.accept_invalid_hostnames);
}

#[test]
fn tls_require_mode_parses() {
    let yaml = r#"
source:
  type: mysql
  url: "mysql://localhost/test"
  tls:
    mode: require
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let tls = cfg.source.tls.unwrap();
    assert_eq!(tls.mode, TlsMode::Require);
    assert!(tls.mode.is_enforced());
}

#[test]
fn tls_disable_mode_is_explicit_optout() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tls:
    mode: disable
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let tls = cfg.source.tls.unwrap();
    assert_eq!(tls.mode, TlsMode::Disable);
    assert!(!tls.mode.is_enforced());
}

#[test]
fn tls_unknown_mode_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tls:
    mode: obviously-not-a-mode
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("mode"),
        "expected error mentioning mode: {msg}"
    );
}

#[test]
fn misplaced_memory_threshold_in_source_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  memory_threshold_mb: 512
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("memory_threshold_mb"),
        "expected field name: {msg}"
    );
}
