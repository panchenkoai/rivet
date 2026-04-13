use std::path::Path;

use crate::error::Result;
use crate::plan::FormatType;

pub fn validate_output(path: &Path, format: FormatType, expected_rows: usize) -> Result<()> {
    let actual = match format {
        FormatType::Parquet => {
            let file = std::fs::File::open(path)?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            let mut count = 0usize;
            for batch in reader {
                count += batch?.num_rows();
            }
            count
        }
        FormatType::Csv => {
            let content = std::fs::read_to_string(path)?;
            let lines = content.lines().count();
            lines.saturating_sub(1)
        }
    };

    if actual != expected_rows {
        anyhow::bail!(
            "validation failed: expected {} rows, got {} in {}",
            expected_rows,
            actual,
            path.display()
        );
    }

    log::info!("validation passed: {} rows verified", actual);
    Ok(())
}
