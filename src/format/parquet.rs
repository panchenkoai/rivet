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
        self.inner.flush()?;
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
