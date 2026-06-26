//! Live: `rivet doctor` advises (never blocks) when the SQL Server source
//! login lacks `VIEW SERVER STATE`, the permission the Tier-2 source-harm probe
//! (`sys.dm_os_wait_stats`) needs.
//!
//! `note_mssql_harm_permission` (src/preflight/doctor.rs) prints a `[note]`
//! *only* when `sample_view_server_state` returns `Some(false)` — i.e. the
//! login authenticated fine but holds no `VIEW SERVER STATE`. The probe uses
//! `HAS_PERMS_BY_NAME`, which any login may call for its own permissions, so it
//! never needs a grant itself.
//!
//! The `sa` test login is sysadmin and holds every permission, so it can only
//! ever exercise the *silent* branch. Proving the advisory branch needs a
//! second, deliberately under-privileged login — created here as a fixture and
//! dropped on guard `Drop`. The negative-control test then reruns the exact
//! same path as `sa` to pin that the note is conditional, not unconditional.

use crate::common::*;
use std::process::Command;

/// The stable identifying phrase of the advisory note. Matching a substring
/// (not the whole sentence) keeps the test robust to wording tweaks in the
/// grant hint while still pinning the *trigger*.
const NOTE_MARKER: &str = "Source-harm metrics need VIEW SERVER STATE";

/// Password for the fixture login. `CHECK_POLICY = OFF` means complexity isn't
/// enforced, but we reuse the known-compliant `sa` password anyway so the
/// `CREATE LOGIN` can't trip a policy on a hardened image.
const LOWPERM_PW: &str = "Rivet_Passw0rd!";

/// A SQL Server login + `rivet`-database user that can connect and read data
/// but holds NO server-level `VIEW SERVER STATE` — exactly the principal whose
/// harm-metric gap doctor must advise about. Server-level logins outlive the
/// session, so this is an RAII guard: it drops the user and login on `Drop`.
struct LowPermLogin {
    login: String,
}

impl LowPermLogin {
    fn create() -> Self {
        let login = unique_name("rivet_lowperm");
        // Idempotent: clear any principal left by a crashed prior run first.
        drop_principal(&login);
        // Server-level login, then a matching rivet-db user with read access.
        // A fresh login gets no VIEW SERVER STATE, which is the whole point.
        mssql_exec(&format!(
            "CREATE LOGIN {login} WITH PASSWORD = '{LOWPERM_PW}', CHECK_POLICY = OFF;"
        ));
        mssql_exec(&format!(
            "CREATE USER {login} FOR LOGIN {login}; ALTER ROLE db_datareader ADD MEMBER {login};"
        ));
        Self { login }
    }

    /// The `sqlserver://` URL that authenticates as this restricted login.
    fn url(&self) -> String {
        format!(
            "sqlserver://{login}:{LOWPERM_PW}@127.0.0.1:1433/rivet",
            login = self.login
        )
    }
}

impl Drop for LowPermLogin {
    fn drop(&mut self) {
        // Teardown must not panic during unwind (pg.rs: "Do NOT panic from
        // Drop") — `mssql_exec` panics on error, so isolate it. A leaked login
        // is a benign test-env artifact; a panic here while already unwinding a
        // failed assertion would abort the whole test process.
        let _ =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop_principal(&self.login)));
    }
}

/// Drop the rivet-db user and the server login for `login`, if present. Ordered
/// user-before-login (a login owning a user can't be dropped). Idempotent.
fn drop_principal(login: &str) {
    mssql_exec(&format!(
        "IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{login}') DROP USER {login};"
    ));
    mssql_exec(&format!(
        "IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{login}') DROP LOGIN {login};"
    ));
}

/// Write a doctor config whose MSSQL source authenticates with `source_url`.
/// Doctor only connects + `SELECT 1` for source auth and write-probes the local
/// destination, so the restricted login needs nothing beyond connect/read.
fn doctor_config(
    source_url: &str,
    table: &str,
    out: &std::path::Path,
) -> (std::path::PathBuf, tempfile::TempDir) {
    let cfg_dir = tempfile::tempdir().unwrap();
    let export = unique_name("doctor_harm");
    let yaml = format!(
        r#"source:
  type: mssql
  url: "{source_url}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export}
    query: "SELECT id, name FROM {table}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {dir}
"#,
        dir = out.display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    (cfg, cfg_dir)
}

/// Run `rivet doctor -c <cfg>` and return its stdout (the note prints there).
fn run_doctor(cfg: &std::path::Path) -> String {
    let out = Command::new(RIVET_BIN)
        .args(["doctor", "-c", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet doctor");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    // Source auth must succeed, else the note path (gated on the Source-OK arm)
    // is never reached and a missing note would be a false negative. Match the
    // "Source auth" line (only the OK arm prints it; the FAIL arm prints
    // "Source <category>:") rather than the "[OK] " marker's exact spacing.
    assert!(
        stdout.contains("Source auth"),
        "doctor must pass source auth for the note path to run; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn doctor_notes_missing_view_server_state_for_restricted_login() {
    require_alive(LiveService::Mssql);

    // `login` drops the SQL principal at scope end (Rust keeps Drop types to
    // scope end, so it stays authenticable while `run_doctor` runs); `_cfg_dir`
    // is a pure on-disk guard for the config file.
    let login = LowPermLogin::create();
    let table = seed_mssql_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let (cfg, _cfg_dir) = doctor_config(&login.url(), table.name(), out.path());

    let stdout = run_doctor(&cfg);
    assert!(
        stdout.contains(NOTE_MARKER),
        "doctor must advise about the missing VIEW SERVER STATE grant for an \
         under-privileged login; stdout:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn doctor_silent_on_view_server_state_for_sysadmin() {
    require_alive(LiveService::Mssql);

    // Negative control: the `sa` login is sysadmin → HAS_PERMS_BY_NAME returns
    // true → the note must NOT print. Without this, a note that fires
    // unconditionally would still pass the positive test above.
    let table = seed_mssql_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let (cfg, _cfg_dir) = doctor_config(MSSQL_URL, table.name(), out.path());

    let stdout = run_doctor(&cfg);
    assert!(
        !stdout.contains(NOTE_MARKER),
        "doctor must stay silent about VIEW SERVER STATE for a login that holds \
         it (sysadmin sa); stdout:\n{stdout}"
    );
}
