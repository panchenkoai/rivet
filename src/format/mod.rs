pub mod csv;
pub mod parquet;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::{CompressionType, FormatType};
use crate::error::Result;

/// Streaming writer: receives one RecordBatch at a time.
pub trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
    /// Approximate bytes written so far (for file-size splitting).
    fn bytes_written(&self) -> u64;
}

pub trait Format {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn std::io::Write + Send>,
    ) -> Result<Box<dyn FormatWriter>>;

    fn file_extension(&self) -> &str;
}

pub fn create_format(
    format_type: FormatType,
    compression: CompressionType,
    compression_level: Option<u32>,
) -> Box<dyn Format> {
    match format_type {
        FormatType::Csv => Box::new(csv::CsvFormat),
        FormatType::Parquet => {
            Box::new(parquet::ParquetFormat::new(compression, compression_level))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(schema: &Arc<Schema>) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )
        .unwrap()
    }

    #[test]
    fn create_format_csv_extension_and_roundtrip() {
        let schema = schema();
        let fmt = create_format(FormatType::Csv, CompressionType::None, None);
        assert_eq!(fmt.file_extension(), "csv");
        let mut w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch(&schema)).unwrap();
        w.finish().unwrap();
    }

    #[test]
    fn create_format_parquet_extension_and_roundtrip() {
        let schema = schema();
        let fmt = create_format(FormatType::Parquet, CompressionType::Zstd, None);
        assert_eq!(fmt.file_extension(), "parquet");
        let mut w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch(&schema)).unwrap();
        w.finish().unwrap();
    }

    #[test]
    fn create_format_parquet_uncompressed_finish_ok() {
        let schema = schema();
        let fmt = create_format(FormatType::Parquet, CompressionType::None, None);
        let w = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.finish().unwrap();
    }
}
