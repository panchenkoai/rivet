pub mod mysql;
pub(crate) mod pg_numeric_wire;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tls;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, CursorState, TypeMapping};

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
    ///
    /// `column_overrides` carries per-column type declarations from `rivet.yaml`
    /// (`exports[].columns:`). The driver applies them during schema building so
    /// that e.g. a `NUMERIC` column without declared precision can still be
    /// exported as `Decimal128(18,2)` when the user has stated the type explicitly.
    fn export(
        &mut self,
        query: &str,
        incremental: Option<&IncrementalCursorPlan>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        column_overrides: &ColumnOverrides,
        sink: &mut dyn BatchSink,
    ) -> Result<()>;

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;

    /// Return `TypeMapping` for every column in `query` without fetching rows.
    ///
    /// Used by `rivet check --type-report` to show the full type provenance
    /// (source native type → RivetType → Arrow type → fidelity) before export.
    /// Implementations execute `SELECT * FROM (...) AS _q LIMIT 0` so only
    /// server-side type metadata is transferred.
    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>>;
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
