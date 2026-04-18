pub mod mysql;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tls;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

/// Receives schema and batches from a source, one at a time.
pub trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

pub trait Source: Send {
    /// Execute `query` (already materialized by `resolve_query`) and stream batches into `sink`.
    ///
    /// `incremental` / `cursor` are handed to the source driver so it can wrap `query` with
    /// the dialect-specific incremental predicate via [`crate::source::query::build_incremental_query`].
    fn export(
        &mut self,
        query: &str,
        incremental: Option<&IncrementalCursorPlan>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        sink: &mut dyn BatchSink,
    ) -> Result<()>;

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
}

pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    use crate::config::SourceType;
    let url = config.resolve_url()?;
    warn_if_tls_disabled(config);
    match config.source_type {
        SourceType::Postgres => Ok(Box::new(postgres::PostgresSource::connect_with_tls(
            &url,
            config.tls.as_ref(),
        )?)),
        SourceType::Mysql => Ok(Box::new(mysql::MysqlSource::connect_with_tls(
            &url,
            config.tls.as_ref(),
        )?)),
    }
}

/// One-time nudge to enable TLS when the current config connects in plaintext.
/// Emitted at `warn` level so operators see it even at the default log level.
pub(crate) fn warn_if_tls_disabled(config: &SourceConfig) {
    let enforced = config.tls.as_ref().is_some_and(|t| t.mode.is_enforced());
    if !enforced {
        log::warn!(
            "source: TLS is not enforced — credentials and result rows cross the network in plaintext. \
             Add `source.tls.mode: verify-full` (with `ca_file:` if your CA is private) to enable transport security."
        );
    }
}
