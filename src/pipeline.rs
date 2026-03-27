use std::io::BufWriter;
use std::time::Duration;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::destination;
use crate::error::Result;
use crate::format;
use crate::source;
use crate::state::StateStore;
use crate::tuning::SourceTuning;

pub fn run(config_path: &str, export_name: Option<&str>) -> Result<()> {
    let config = Config::load(config_path)?;
    let tuning = SourceTuning::from_config(config.source.tuning.as_ref());
    log::info!("source tuning: {}", tuning);

    let state = StateStore::open(config_path)?;
    let mut src = source::create_source(&config.source)?;

    let exports: Vec<&ExportConfig> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    for export in exports {
        log::info!("starting export '{}'", export.name);
        if let Err(e) = run_export_with_retry(&mut *src, &state, export, &tuning) {
            log::error!("export '{}' failed: {:#}", export.name, e);
        }
    }

    Ok(())
}

fn run_export_with_retry(
    src: &mut dyn source::Source,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
) -> Result<()> {
    let mut last_err = None;

    for attempt in 0..=tuning.max_retries {
        if attempt > 0 {
            let backoff = tuning.retry_backoff_ms * 2u64.pow(attempt - 1);
            log::warn!(
                "export '{}': retry {}/{} in {}ms ({})",
                export.name,
                attempt,
                tuning.max_retries,
                backoff,
                last_err.as_ref().map(|e: &anyhow::Error| format!("{:#}", e)).unwrap_or_default(),
            );
            std::thread::sleep(Duration::from_millis(backoff));
        }

        match run_export(src, state, export, tuning) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < tuning.max_retries && is_transient(&e) {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("export failed after retries")))
}

pub(crate) fn is_transient(err: &anyhow::Error) -> bool {
    let msg = format!("{:#}", err).to_lowercase();
    msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("lock timeout")
        || msg.contains("canceling statement")
        || msg.contains("gone away")
        || msg.contains("lost connection")
}

fn run_export(
    src: &mut dyn source::Source,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
) -> Result<()> {
    let cursor_state = if export.mode == ExportMode::Incremental {
        Some(state.get(&export.name)?)
    } else {
        None
    };

    let cursor_column = if export.mode == ExportMode::Incremental {
        export.cursor_column.as_deref()
    } else {
        None
    };

    let schema = src.begin_query(
        &export.query,
        cursor_column,
        cursor_state.as_ref(),
        tuning,
    )?;

    let fmt = format::create_format(export.format);
    let tmp = tempfile::NamedTempFile::new()?;
    let file = tmp.as_file().try_clone()?;
    let buf_writer = BufWriter::new(file);
    let mut writer = fmt.create_writer(&schema, Box::new(buf_writer))?;

    let mut total_rows: usize = 0;
    let mut last_batch: Option<arrow::record_batch::RecordBatch> = None;

    while let Some(batch) = src.fetch_next()? {
        total_rows += batch.num_rows();
        writer.write_batch(&batch)?;
        last_batch = Some(batch);
    }

    writer.finish()?;
    src.close_query()?;

    log::info!("export '{}': {} rows written", export.name, total_rows);

    if total_rows == 0 {
        log::info!("export '{}': no data to export", export.name);
        return Ok(());
    }

    let file_name = build_file_name(&export.name, fmt.file_extension());
    let dest = destination::create_destination(&export.destination)?;
    dest.write(tmp.path(), &file_name)?;

    if export.mode == ExportMode::Incremental {
        if let Some(cursor_col) = &export.cursor_column {
            if let Some(batch) = &last_batch {
                if let Some(last_val) = extract_last_cursor_value(batch, cursor_col, &schema) {
                    state.update(&export.name, &last_val)?;
                    log::info!("export '{}': cursor updated to '{}'", export.name, last_val);
                }
            }
        }
    }

    log::info!("export '{}' completed successfully", export.name);
    Ok(())
}

pub(crate) fn build_file_name(export_name: &str, extension: &str) -> String {
    let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    format!("{}_{}.{}", export_name, now, extension)
}

pub(crate) fn extract_last_cursor_value(
    batch: &arrow::record_batch::RecordBatch,
    cursor_column: &str,
    schema: &arrow::datatypes::SchemaRef,
) -> Option<String> {
    let col_idx = schema.index_of(cursor_column).ok()?;
    let array = batch.column(col_idx);
    let last_row = batch.num_rows().checked_sub(1)?;

    if array.is_null(last_row) {
        return None;
    }

    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    match array.data_type() {
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>()?;
            Some(arr.value(last_row).to_string())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()?;
            Some(arr.value(last_row).to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            Some(arr.value(last_row).to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            Some(arr.value(last_row).to_string())
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            Some(arr.value(last_row).to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?;
            let micros = arr.value(last_row);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>()?;
            let days = arr.value(last_row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?
                + chrono::Duration::days(days as i64);
            Some(date.to_string())
        }
        _ => {
            log::warn!(
                "cannot extract cursor value for type {:?}",
                array.data_type()
            );
            None
        }
    }
}

pub fn show_state(config_path: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let states = state.list_all()?;
    if states.is_empty() {
        println!("No export state recorded yet.");
        return Ok(());
    }
    println!("{:<30} {:<40} {}", "EXPORT", "LAST CURSOR", "LAST RUN");
    println!("{}", "-".repeat(90));
    for s in &states {
        println!(
            "{:<30} {:<40} {}",
            s.export_name,
            s.last_cursor_value.as_deref().unwrap_or("-"),
            s.last_run_at.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

pub fn reset_state(config_path: &str, export_name: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    state.reset(export_name)?;
    println!("State reset for export '{}'", export_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_build_file_name() {
        let name = build_file_name("users", "csv");
        assert!(name.starts_with("users_"));
        assert!(name.ends_with(".csv"));
        assert!(name.len() > "users_.csv".len());
    }

    fn make_int64_batch(col_name: &str, values: &[i64]) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(col_name, DataType::Int64, false),
        ]));
        let array = Int64Array::from(values.to_vec());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        (schema, batch)
    }

    #[test]
    fn test_extract_cursor_int64() {
        let (schema, batch) = make_int64_batch("id", &[10, 20, 30]);
        let result = extract_last_cursor_value(&batch, "id", &schema);
        assert_eq!(result, Some("30".to_string()));
    }

    #[test]
    fn test_extract_cursor_utf8() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
        ]));
        let array = StringArray::from(vec!["alice", "bob", "carol"]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let result = extract_last_cursor_value(&batch, "name", &schema);
        assert_eq!(result, Some("carol".to_string()));
    }

    #[test]
    fn test_extract_cursor_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        let micros = 1_700_000_000_000_000i64;
        let array = TimestampMicrosecondArray::from(vec![micros]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let result = extract_last_cursor_value(&batch, "ts", &schema);
        assert!(result.is_some());
        assert!(result.unwrap().starts_with("2023-11-14"));
    }

    #[test]
    fn test_extract_cursor_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let array = Int64Array::from(Vec::<i64>::new());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let result = extract_last_cursor_value(&batch, "id", &schema);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_transient_matches() {
        assert!(is_transient(&anyhow::anyhow!("statement timed out")));
        assert!(is_transient(&anyhow::anyhow!("connection reset by peer")));
        assert!(is_transient(&anyhow::anyhow!("MySQL server has gone away")));
        assert!(is_transient(&anyhow::anyhow!("lock timeout exceeded")));
    }

    #[test]
    fn test_is_transient_rejects() {
        assert!(!is_transient(&anyhow::anyhow!("syntax error at or near SELECT")));
        assert!(!is_transient(&anyhow::anyhow!("permission denied for table users")));
    }
}
