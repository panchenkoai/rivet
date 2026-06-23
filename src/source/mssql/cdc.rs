//! SQL Server CDC — the structural outlier. There is **no** client-streamable
//! transaction log over TDS (no binlog / logical-slot analog). The supported path
//! is to **poll the `cdc.*` change tables** that the server's capture Agent job
//! has already extracted from the log, by an LSN watermark — plain T-SQL over the
//! `tiberius` driver rivet already uses (no CDC-specific crate exists, or is
//! needed).
//!
//! This is the *reader seam* only — not yet wired to a `mode: cdc` / CLI. It reads
//! the per-change `__$operation` (1=delete, 2=insert, 3=update-before,
//! 4=update-after) for a capture instance's `[min_lsn, max_lsn]` window. The
//! per-column value extraction (generic, via `cdc.captured_columns`) and a durable
//! watermark loop are the deferred CDC-semantics work.
//!
//! Prereqs (heavier than MySQL/PG): CDC enabled (`sp_cdc_enable_db` +
//! `sp_cdc_enable_table`), **SQL Server Agent running** (the capture job), and a
//! supported edition (Standard/Enterprise/Developer, not Express). A stalled Agent
//! freezes the change tables *and pins log truncation* — a real reader must detect
//! a non-advancing `fn_cdc_get_max_lsn`, not read "no rows" as "no changes".
#![allow(dead_code)] // reader seam; the CLI / mode wiring lands in a later increment.

use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::error::Result;

/// `cdc.fn_cdc_get_all_changes_*` `__$operation` codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Delete,
    Insert,
    UpdateBefore,
    UpdateAfter,
    Other,
}

impl ChangeOp {
    fn from_code(op: i32) -> Self {
        match op {
            1 => ChangeOp::Delete,
            2 => ChangeOp::Insert,
            3 => ChangeOp::UpdateBefore,
            4 => ChangeOp::UpdateAfter,
            _ => ChangeOp::Other,
        }
    }
}

/// One change from a CDC change table (operation only, for the seam).
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub op: ChangeOp,
}

/// Connection params for a CDC poller. (rivet's `SourceConfig` supplies these in
/// the real wiring; the seam takes them directly.)
pub(crate) struct MssqlCdcReader {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl MssqlCdcReader {
    /// Poll all changes currently in `capture_instance`'s change table — the
    /// `[fn_cdc_get_min_lsn, fn_cdc_get_max_lsn]` window. One-shot; a production
    /// reader advances a persisted watermark instead of re-reading from min.
    ///
    /// `capture_instance` is interpolated into the function name (CDC names the
    /// function `cdc.fn_cdc_get_all_changes_<instance>`, which can't be a bind
    /// parameter), so it is validated as a plain identifier first.
    pub(crate) fn poll(&self, capture_instance: &str) -> Result<Vec<ChangeEvent>> {
        if !capture_instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            anyhow::bail!("invalid CDC capture instance name: {capture_instance:?}");
        }
        let sql = format!(
            "SELECT __$operation AS op \
             FROM cdc.fn_cdc_get_all_changes_{capture_instance}( \
                  sys.fn_cdc_get_min_lsn('{capture_instance}'), \
                  sys.fn_cdc_get_max_lsn(), N'all') \
             ORDER BY __$start_lsn, __$seqval"
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let mut client = self.connect().await?;
            let rows = client.simple_query(sql).await?.into_first_result().await?;
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let op: i32 = r.get("op").unwrap_or_default();
                out.push(ChangeEvent {
                    op: ChangeOp::from_code(op),
                });
            }
            Ok(out)
        })
    }

    async fn connect(&self) -> Result<Client<Compat<TcpStream>>> {
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.authentication(AuthMethod::sql_server(&self.user, &self.password));
        config.encryption(EncryptionLevel::Required);
        config.trust_cert();
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Ok(Client::connect(config, tcp.compat_write()).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reader() -> MssqlCdcReader {
        MssqlCdcReader {
            host: "127.0.0.1".into(),
            port: 1433,
            database: "rivet".into(),
            user: "sa".into(),
            password: "Rivet_Passw0rd!".into(),
        }
    }

    /// Run arbitrary T-SQL via the reader's connection (test setup helper).
    fn exec(r: &MssqlCdcReader, sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = r.connect().await.unwrap();
            for batch in sql.split(";\n") {
                if !batch.trim().is_empty() {
                    c.simple_query(batch)
                        .await
                        .unwrap()
                        .into_results()
                        .await
                        .unwrap();
                }
            }
        });
    }

    #[test]
    #[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC enabled"]
    fn poll_reads_change_operations() {
        let r = reader();
        // Fresh table + CDC capture instance.
        exec(
            &r,
            "IF OBJECT_ID('cdc_unit','U') IS NOT NULL DROP TABLE cdc_unit;\n\
             CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT);\n\
             IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;\n\
             IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_cdc_unit') \
               EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit';\n\
             EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='cdc_unit',@role_name=NULL,@capture_instance='dbo_cdc_unit',@supports_net_changes=0",
        );
        // Mutate, then let the capture Agent job scan the log (~5 s cycle).
        exec(
            &r,
            "INSERT INTO cdc_unit VALUES (1,10);\n\
             UPDATE cdc_unit SET v=20 WHERE id=1;\n\
             DELETE FROM cdc_unit WHERE id=1",
        );
        std::thread::sleep(std::time::Duration::from_secs(8));

        let ops: Vec<ChangeOp> = r
            .poll("dbo_cdc_unit")
            .unwrap()
            .into_iter()
            .map(|e| e.op)
            .collect();
        exec(
            &r,
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit'",
        );
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::UpdateAfter, ChangeOp::Delete],
            "CDC change table must yield insert(2), update-after(4), delete(1)"
        );
    }
}
