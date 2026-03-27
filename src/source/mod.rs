pub mod mysql;
pub mod postgres;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

/// Receives schema and batches from a source, one at a time.
pub trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

pub trait Source: Send {
    fn export(
        &mut self,
        query: &str,
        cursor_column: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        sink: &mut dyn BatchSink,
    ) -> Result<()>;

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
}

pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    use crate::config::SourceType;
    let url = config.resolve_url()?;
    match config.source_type {
        SourceType::Postgres => Ok(Box::new(postgres::PostgresSource::connect(&url)?)),
        SourceType::Mysql => Ok(Box::new(mysql::MysqlSource::connect(&url)?)),
    }
}
