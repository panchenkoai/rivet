use std::io::Write;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::Result;

pub struct ParquetFormat;

pub struct ParquetFormatWriter {
    inner: ArrowWriter<Box<dyn Write + Send>>,
}

impl super::Format for ParquetFormat {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn super::FormatWriter>> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let inner = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;
        Ok(Box::new(ParquetFormatWriter { inner }))
    }

    fn file_extension(&self) -> &str {
        "parquet"
    }
}

impl super::FormatWriter for ParquetFormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)?;
        self.inner.flush()?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        self.inner.close()?;
        Ok(())
    }
}
