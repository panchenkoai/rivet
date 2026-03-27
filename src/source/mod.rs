pub mod mysql;
pub mod postgres;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

pub trait Source {
    fn begin_query(
        &mut self,
        query: &str,
        cursor_column: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
    ) -> Result<SchemaRef>;

    fn fetch_next(&mut self) -> Result<Option<RecordBatch>>;

    fn close_query(&mut self) -> Result<()>;
}

pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    use crate::config::SourceType;
    match config.source_type {
        SourceType::Postgres => Ok(Box::new(postgres::PostgresSource::connect(&config.url)?)),
        SourceType::Mysql => Ok(Box::new(mysql::MysqlSource::connect(&config.url)?)),
    }
}
