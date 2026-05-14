use std::io::Write;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::config::CompressionType;
use crate::error::Result;

pub struct ParquetFormat {
    compression: CompressionType,
    compression_level: Option<u32>,
}

impl ParquetFormat {
    pub fn new(compression: CompressionType, compression_level: Option<u32>) -> Self {
        Self {
            compression,
            compression_level,
        }
    }

    fn build_compression(&self) -> Compression {
        match self.compression {
            CompressionType::Zstd => {
                let level = self.compression_level.unwrap_or(3) as i32;
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or_default())
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Gzip => {
                let level = self.compression_level.unwrap_or(6);
                Compression::GZIP(GzipLevel::try_new(level).unwrap_or_default())
            }
            CompressionType::Lz4 => Compression::LZ4,
            CompressionType::None => Compression::UNCOMPRESSED,
        }
    }
}

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
            .set_compression(self.build_compression())
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
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        self.inner.close()?;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.inner.bytes_written() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn int64_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn one_batch(schema: &Arc<Schema>) -> arrow::record_batch::RecordBatch {
        arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap()
    }

    fn make_writer(
        compression: CompressionType,
        level: Option<u32>,
    ) -> Box<dyn crate::format::FormatWriter> {
        let schema = int64_schema();
        ParquetFormat::new(compression, level)
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .expect("create_writer should succeed")
    }

    // ── file_extension ───────────────────────────────────────────────────────

    #[test]
    fn file_extension_is_parquet() {
        assert_eq!(
            ParquetFormat::new(CompressionType::None, None).file_extension(),
            "parquet"
        );
    }

    // ── create_writer succeeds for every compression codec ───────────────────

    #[test]
    fn create_writer_zstd_default_level_succeeds() {
        let _ = make_writer(CompressionType::Zstd, None);
    }

    #[test]
    fn create_writer_zstd_explicit_level_succeeds() {
        let _ = make_writer(CompressionType::Zstd, Some(9));
    }

    #[test]
    fn create_writer_snappy_succeeds() {
        let _ = make_writer(CompressionType::Snappy, None);
    }

    #[test]
    fn create_writer_gzip_succeeds() {
        let _ = make_writer(CompressionType::Gzip, None);
    }

    #[test]
    fn create_writer_lz4_succeeds() {
        let _ = make_writer(CompressionType::Lz4, None);
    }

    #[test]
    fn create_writer_uncompressed_succeeds() {
        let _ = make_writer(CompressionType::None, None);
    }

    // ── write_batch + finish ─────────────────────────────────────────────────

    #[test]
    fn write_batch_and_finish_returns_ok() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::Zstd, None);
        // Pass Vec by value — avoids &mut T 'static lifetime requirement.
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap(); // finalizes the parquet file footer
    }

    #[test]
    fn finish_without_write_produces_valid_empty_parquet() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None);
        // finish() on a writer with no batches should not panic or error
        let writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.finish().unwrap();
    }
}
