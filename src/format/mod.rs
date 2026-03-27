pub mod csv;
pub mod parquet;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::FormatType;
use crate::error::Result;

/// Streaming writer: receives one RecordBatch at a time.
pub trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
}

pub trait Format {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn std::io::Write + Send>,
    ) -> Result<Box<dyn FormatWriter>>;

    fn file_extension(&self) -> &str;
}

pub fn create_format(format_type: FormatType) -> Box<dyn Format> {
    match format_type {
        FormatType::Csv => Box::new(csv::CsvFormat),
        FormatType::Parquet => Box::new(parquet::ParquetFormat),
    }
}
