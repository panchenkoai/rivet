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
    /// Rows per Parquet row group. `None` = use library default (1,048,576).
    row_group_rows: Option<usize>,
}

impl ParquetFormat {
    pub fn new(
        compression: CompressionType,
        compression_level: Option<u32>,
        row_group_rows: Option<usize>,
    ) -> Self {
        Self {
            compression,
            compression_level,
            row_group_rows,
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
    ) -> Result<Box<dyn super::FormatWriter + Send>> {
        // OPT-5: pin a version-independent `created_by`. By default parquet-rs
        // stamps each file with its own version (e.g. "parquet-rs version
        // 58.0.0"); that string changes on a lib bump, so identical rows would
        // produce different bytes — breaking the manifest `content_fingerprint`
        // as a *cross-release* dedup key. A constant keeps identical rows
        // byte-identical across rivet/parquet-rs versions. Writer provenance
        // lives in the run manifest/journal, not the file footer.
        let mut builder = WriterProperties::builder()
            .set_compression(self.build_compression())
            .set_created_by("rivet".to_string());
        if self.row_group_rows.is_some() {
            builder = builder.set_max_row_group_row_count(self.row_group_rows);
        }
        let props = builder.build();

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
        ParquetFormat::new(compression, level, None)
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .expect("create_writer should succeed")
    }

    // ── file_extension ───────────────────────────────────────────────────────

    #[test]
    fn file_extension_is_parquet() {
        assert_eq!(
            ParquetFormat::new(CompressionType::None, None, None).file_extension(),
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
        let fmt = ParquetFormat::new(CompressionType::Zstd, None, None);
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
        let fmt = ParquetFormat::new(CompressionType::None, None, None);
        // finish() on a writer with no batches should not panic or error
        let writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.finish().unwrap();
    }

    // ── row group size ───────────────────────────────────────────────────────

    #[test]
    fn row_group_rows_none_uses_library_default() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, None);
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn row_group_rows_some_succeeds() {
        let schema = int64_schema();
        let fmt = ParquetFormat::new(CompressionType::None, None, Some(100));
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&one_batch(&schema)).unwrap();
        writer.finish().unwrap();
    }

    // ── OPT-5: byte-determinism for the manifest content_fingerprint ──────────

    fn write_batch_to_bytes(compression: CompressionType) -> Vec<u8> {
        let schema = int64_schema();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tmp.path()).unwrap();
        let mut w = ParquetFormat::new(compression, None, None)
            .create_writer(&schema, Box::new(file))
            .unwrap();
        w.write_batch(&one_batch(&schema)).unwrap();
        w.finish().unwrap();
        std::fs::read(tmp.path()).unwrap()
    }

    #[test]
    fn output_is_byte_deterministic_for_identical_rows() {
        // Identical rows must produce byte-identical Parquet so the manifest
        // `content_fingerprint` (xxh3 of the file bytes) is a stable dedup key.
        let a = write_batch_to_bytes(CompressionType::Zstd);
        let b = write_batch_to_bytes(CompressionType::Zstd);
        assert_eq!(a, b, "identical rows must yield byte-identical parquet");
    }

    #[test]
    fn created_by_is_pinned_and_version_free() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let bytes = write_batch_to_bytes(CompressionType::None);
        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let created_by = reader.metadata().file_metadata().created_by();
        assert_eq!(
            created_by,
            Some("rivet"),
            "created_by must be the pinned constant"
        );
        // Must not leak the parquet-rs version — that's the cross-release drift
        // that would break the fingerprint as a dedup key.
        let cb = created_by.unwrap();
        assert!(
            !cb.contains("version") && !cb.contains("parquet"),
            "created_by must not embed the library version: {cb:?}"
        );
    }
}
