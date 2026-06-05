//! **Layer: Execution** — MSSQL / SQL Server source engine.
//!
//! Third SQL engine after PostgreSQL and MySQL. The `tiberius` driver is
//! async (tokio); the `Source` trait is sync `&mut self` (ADR-0011), so each
//! `MssqlSource` owns a current-thread `tokio` runtime and `block_on`s every
//! driver call — the same shape the rest of the pipeline expects, no async
//! leaking into the runner.
//!
//! Dialect deltas vs PG/MySQL (all routed through the shared seams):
//! - identifier quoting `[col]` (`sql::quote_ident`)
//! - pagination `OFFSET … FETCH NEXT … ROWS ONLY` (needs `ORDER BY`)
//! - cursor literal `N'…'` with `''` escaping (`query::cursor_rhs`)
//! - introspection via `sys.*` catalog views
//!
//! NOTE: scaffold — connect/export/type-mapping land in follow-up commits.
//! Every method currently surfaces an explicit "not yet implemented" error so
//! the engine is wired through all dispatch seams and compiles, without
//! pretending to work.

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::{BatchSink, ExportRequest, Source, TableIntrospection};
use crate::types::{ColumnOverrides, TypeMapping};

/// SQL Server source. Owns the async driver + the runtime that drives it.
pub(crate) struct MssqlSource {
    // Filled in the driver commit: `rt: tokio::runtime::Runtime`,
    // `client: tiberius::Client<Compat<TcpStream>>`, `database: String`.
    _private: (),
}

impl MssqlSource {
    /// Connect to SQL Server, honouring the shared `TlsConfig` (same
    /// `native-tls` stack as PG/MySQL). `url` is the resolved
    /// `sqlserver://user:pass@host:port/db` form.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        let _ = (url, tls);
        anyhow::bail!(
            "mssql source: connect not yet implemented (scaffold). \
             Driver lands in a follow-up commit."
        )
    }
}

impl Source for MssqlSource {
    fn export(&mut self, request: &ExportRequest<'_>, _sink: &mut dyn BatchSink) -> Result<()> {
        let _ = request;
        anyhow::bail!("mssql source: export not yet implemented (scaffold)")
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let _ = sql;
        anyhow::bail!("mssql source: query_scalar not yet implemented (scaffold)")
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        let _ = (query, column_overrides);
        anyhow::bail!("mssql source: type_mappings not yet implemented (scaffold)")
    }
}

/// Probe `sys.*` for the stats chunked-mode planning needs (ADR-0015 seam).
/// Mirrors `introspect_pg_table_for_chunking` / `introspect_mysql_table_for_chunking`.
pub(crate) fn introspect_mssql_table_for_chunking(
    url: &str,
    tls: Option<&TlsConfig>,
    qualified_table: &str,
) -> Result<TableIntrospection> {
    let _ = (url, tls, qualified_table);
    anyhow::bail!("mssql introspection not yet implemented (scaffold)")
}
