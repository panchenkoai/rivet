//! SQL Server CDC adapter — `cdc.*` change-table poll → canonical
//! [`cdc::ChangeEvent`]. The structural outlier: no client-streamable log over
//! TDS, so this polls the change tables the server's capture Agent extracted, by
//! LSN window — plain T-SQL over `tiberius` (no CDC-specific crate exists or is
//! needed).
//!
//! `next_change` polls the change function once into a buffer and drains it; a
//! continuous daemon wraps [`crate::source::cdc::run`] in an outer poll loop. The
//! runtime + connection are held by the stream (paid once, not per poll).
//!
//! Pre-images / typed values are deferred — this adapter carries op + schema +
//! table + position; the per-column extraction (via `cdc.captured_columns`) is
//! the SQL Server completion step.
//!
//! Prereqs (heaviest of the three): CDC enabled, **SQL Server Agent running**,
//! supported edition (not Express). A stalled Agent freezes the change tables AND
//! pins log truncation — a real reader must detect a non-advancing max LSN.
//!
//! `#![allow(dead_code)]`: consumed by `cli::dispatch` (binary crate); the lib
//! crate compiles `source` for tests but has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::VecDeque;

use serde_json::json;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::error::Result;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};

/// Connection parameters for a SQL Server CDC poll stream.
pub(crate) struct MssqlCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// CDC capture instance, e.g. `dbo_orders`. Validated as an identifier
    /// because it is interpolated into the change-function name (which can't be a
    /// bind parameter).
    pub capture_instance: String,
}

/// Polls a CDC change table and yields canonical changes.
pub(crate) struct MssqlChangeStream {
    rt: tokio::runtime::Runtime,
    client: Client<Compat<TcpStream>>,
    capture_instance: String,
    schema: String,
    table: String,
    pending: VecDeque<ChangeEvent>,
    drained: bool,
}

impl MssqlChangeStream {
    /// Connect and bind to a capture instance. Holds the runtime + connection for
    /// the life of the stream (folds the per-poll runtime/connect smell away).
    pub(crate) fn open(cfg: &MssqlCdcConfig) -> Result<Self> {
        if !cfg
            .capture_instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            anyhow::bail!(
                "invalid CDC capture instance name: {:?}",
                cfg.capture_instance
            );
        }
        // Heuristic schema/table from `<schema>_<table>` — robust resolution via
        // cdc.change_tables metadata is the SQL Server completion step.
        let (schema, table) = cfg
            .capture_instance
            .split_once('_')
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or_else(|| (String::new(), cfg.capture_instance.clone()));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = rt.block_on(connect(cfg))?;
        Ok(Self {
            rt,
            client,
            capture_instance: cfg.capture_instance.clone(),
            schema,
            table,
            pending: VecDeque::new(),
            drained: false,
        })
    }

    /// Poll the change table once over `[min_lsn, max_lsn]` into `pending`.
    fn fill(&mut self) -> Result<()> {
        let Self {
            rt,
            client,
            capture_instance,
            schema,
            table,
            pending,
            ..
        } = self;
        let sql = format!(
            "SELECT CONVERT(VARCHAR(20), __$start_lsn, 2) AS lsn, __$operation AS op \
             FROM cdc.fn_cdc_get_all_changes_{ci}( \
                  sys.fn_cdc_get_min_lsn('{ci}'), sys.fn_cdc_get_max_lsn(), N'all') \
             ORDER BY __$start_lsn, __$seqval",
            ci = capture_instance
        );
        let rows =
            rt.block_on(async { client.simple_query(sql).await?.into_first_result().await })?;
        for r in rows {
            let op_code: i32 = r.get("op").unwrap_or_default();
            let Some(op) = map_op(op_code) else { continue };
            let lsn: &str = r.get("lsn").unwrap_or("");
            pending.push_back(ChangeEvent {
                op,
                schema: schema.clone(),
                table: table.clone(),
                before: None,
                after: None,
                position: Position(json!({ "lsn": lsn })),
                // The change table only ever holds already-committed changes.
                committed: true,
            });
        }
        Ok(())
    }
}

impl ChangeStream for MssqlChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        if self.pending.is_empty() && !self.drained {
            self.drained = true;
            if let Err(e) = self.fill() {
                return Some(Err(e));
            }
        }
        self.pending.pop_front().map(Ok)
    }
}

/// `__$operation` → canonical op. 1=delete, 2=insert, 4=update-after; 3 (update
/// before-image) is skipped — under `N'all'` an update yields only op 4.
fn map_op(code: i32) -> Option<ChangeOp> {
    match code {
        1 => Some(ChangeOp::Delete),
        2 => Some(ChangeOp::Insert),
        4 => Some(ChangeOp::Update),
        _ => None,
    }
}

async fn connect(cfg: &MssqlCdcConfig) -> Result<Client<Compat<TcpStream>>> {
    let mut config = Config::new();
    config.host(&cfg.host);
    config.port(cfg.port);
    config.database(&cfg.database);
    config.authentication(AuthMethod::sql_server(&cfg.user, &cfg.password));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(capture_instance: &str) -> MssqlCdcConfig {
        MssqlCdcConfig {
            host: "127.0.0.1".into(),
            port: 1433,
            database: "rivet".into(),
            user: "sa".into(),
            password: "Rivet_Passw0rd!".into(),
            capture_instance: capture_instance.into(),
        }
    }

    /// Run arbitrary T-SQL on a throwaway connection (test setup helper).
    fn exec(sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = connect(&cfg("dbo_cdc_unit")).await.unwrap();
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
    fn streams_change_operations() {
        exec(
            "IF OBJECT_ID('cdc_unit','U') IS NOT NULL DROP TABLE cdc_unit;\n\
             CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT);\n\
             IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;\n\
             IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_cdc_unit') \
               EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit';\n\
             EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='cdc_unit',@role_name=NULL,@capture_instance='dbo_cdc_unit',@supports_net_changes=0",
        );
        exec(
            "INSERT INTO cdc_unit VALUES (1,10);\n\
             UPDATE cdc_unit SET v=20 WHERE id=1;\n\
             DELETE FROM cdc_unit WHERE id=1",
        );
        // let the capture Agent job scan the log (~5 s cycle)
        std::thread::sleep(std::time::Duration::from_secs(8));

        let mut s = MssqlChangeStream::open(&cfg("dbo_cdc_unit")).unwrap();
        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            ops.push(ev.unwrap().op);
        }
        exec(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit'",
        );
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "CDC change table must yield insert(2), update-after(4), delete(1)"
        );
    }
}
