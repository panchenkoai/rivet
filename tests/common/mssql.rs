//! MSSQL test helpers: a tiny tiberius-backed executor for fixture setup.
//!
//! Mirrors [`super::mysql`] / [`super::pg`] but the SQL Server driver is async,
//! so each call spins a current-thread runtime and `block_on`s — fixture setup
//! is infrequent, so a per-call runtime is fine. Connection parameters match
//! the `mssql` service in `docker-compose.yaml`.

#![allow(dead_code)]

use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

async fn connect() -> Client<Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(1433);
    config.database("rivet");
    config.authentication(AuthMethod::sql_server("sa", "Rivet_Passw0rd!"));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("mssql: tcp connect (is the `mssql` service up?)");
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .expect("mssql: login")
}

/// Run T-SQL against the `rivet` database. `GO`-delimited batches run in order;
/// statements within a batch run together. Panics on error (test setup).
pub fn mssql_exec(sql: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect().await;
        for batch in split_go(sql) {
            if batch.trim().is_empty() {
                continue;
            }
            client
                .simple_query(batch.as_str())
                .await
                .expect("mssql: exec batch")
                .into_results()
                .await
                .expect("mssql: drain batch");
        }
    });
}

/// Idempotent table drop for RAII cleanup guards.
pub fn mssql_drop_table(name: &str) {
    mssql_exec(&format!(
        "IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}"
    ));
}

/// Split a script on lines that are exactly `GO` (the sqlcmd batch separator,
/// not a T-SQL keyword) so each batch can be submitted independently.
fn split_go(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in sql.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}
