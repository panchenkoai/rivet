//! The partitions a load touches, estimated from the Parquet footers it is about to
//! load — before any BigQuery job runs (ADR-0034 D4). BigQuery writes at most 4,000
//! partitions per job; a finer granularity than the data's span allows fails only after
//! the free load job ran, with no way to split the load from rivet. Footer statistics
//! give the partition column's range per file for two small range reads each.

use crate::destination::gcs::GcsStore;
use crate::load::plan::{Granularity, PartitionKey, TablePartition};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike};
use parquet::basic::{LogicalType, TimeUnit, TimestampType, Type as PhysicalType};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::statistics::Statistics;

/// BigQuery's cap on partitions one load job may write.
pub(crate) const MAX_PARTITIONS_PER_JOB: i64 = 4000;

/// The unit a partition column's values are stored in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Days,
    Millis,
    Micros,
    Nanos,
    Plain,
}

/// A column's `[lo, hi]` over the files, in its stored unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    lo: i64,
    hi: i64,
    unit: Unit,
}

/// Refuse a load whose Parquet spans more partitions than one BigQuery job may write.
/// A file without statistics for the column skips the check: BigQuery is the backstop.
pub(crate) fn check_partition_budget(
    store: &GcsStore,
    uris: &[String],
    partition: &TablePartition,
) -> Result<()> {
    let Some(column) = partition.key.column() else {
        return Ok(());
    };
    let mut span: Option<Span> = None;
    for uri in uris {
        let (_, key) = crate::load::split_gs_uri(uri)?;
        let meta = read_footer(store, key)
            .with_context(|| format!("reading the Parquet footer of {uri}"))?;
        let Some(file) = column_span(&meta, column) else {
            return Ok(());
        };
        span = Some(match span {
            None => file,
            Some(s) => merge(s, file),
        });
    }
    let Some(span) = span else {
        return Ok(());
    };
    let touched = partitions_touched(&partition.key, span);
    if touched > MAX_PARTITIONS_PER_JOB {
        bail!("{}", over_budget_message(&partition.key, span, touched));
    }
    Ok(())
}

/// The footer of the Parquet object at the bucket-relative `key`: its last 8 bytes name
/// the metadata length, and the metadata sits right before them.
fn read_footer(store: &GcsStore, key: &str) -> Result<ParquetMetaData> {
    let size = store.stat_size(key)?;
    let tail_len = FOOTER_SIZE as u64;
    if size < tail_len {
        bail!("{key} is {size} bytes, too short to be a Parquet file");
    }
    let tail = store.read_range(key, size - tail_len, tail_len)?;
    let tail: [u8; FOOTER_SIZE] = tail
        .as_slice()
        .try_into()
        .context("the Parquet footer tail is not 8 bytes")?;
    let len = FooterTail::try_new(&tail)?.metadata_length() as u64;
    if size < tail_len + len {
        bail!("{key} declares a {len}-byte footer but is {size} bytes long");
    }
    let bytes = store.read_range(key, size - tail_len - len, len)?;
    Ok(ParquetMetaDataReader::decode_metadata(&bytes)?)
}

/// `column`'s min and max over every row group of one file, or `None` when the column
/// is absent, of a type no partition takes, or has no statistics somewhere.
fn column_span(meta: &ParquetMetaData, column: &str) -> Option<Span> {
    let schema = meta.file_metadata().schema_descr();
    let idx = schema.columns().iter().position(|c| c.name() == column)?;
    let descr = schema.column(idx);
    let unit = stored_unit(descr.logical_type_ref(), descr.physical_type())?;
    let mut span: Option<Span> = None;
    for rg in meta.row_groups() {
        let (lo, hi) = match rg.column(idx).statistics()? {
            Statistics::Int32(s) => (i64::from(*s.min_opt()?), i64::from(*s.max_opt()?)),
            Statistics::Int64(s) => (*s.min_opt()?, *s.max_opt()?),
            _ => return None,
        };
        let file = Span { lo, hi, unit };
        span = Some(match span {
            None => file,
            Some(s) => merge(s, file),
        });
    }
    span
}

/// The unit a partitionable column's statistics are in, by its Parquet type.
fn stored_unit(logical: Option<&LogicalType>, physical: PhysicalType) -> Option<Unit> {
    match (logical, physical) {
        (Some(LogicalType::Date), _) => Some(Unit::Days),
        (Some(LogicalType::Timestamp(TimestampType { unit, .. })), _) => Some(match unit {
            TimeUnit::MILLIS => Unit::Millis,
            TimeUnit::MICROS => Unit::Micros,
            TimeUnit::NANOS => Unit::Nanos,
        }),
        (None, PhysicalType::INT64) | (Some(LogicalType::Integer { .. }), PhysicalType::INT64) => {
            Some(Unit::Plain)
        }
        _ => None,
    }
}

/// The span covering both.
fn merge(a: Span, b: Span) -> Span {
    Span {
        lo: a.lo.min(b.lo),
        hi: a.hi.max(b.hi),
        unit: a.unit,
    }
}

/// A stored value as Unix seconds.
fn to_seconds(v: i64, unit: Unit) -> i64 {
    match unit {
        Unit::Days => v * 86_400,
        Unit::Millis => v.div_euclid(1_000),
        Unit::Micros => v.div_euclid(1_000_000),
        Unit::Nanos => v.div_euclid(1_000_000_000),
        Unit::Plain => v,
    }
}

/// How many partitions of `key` the values in `span` fall into.
fn partitions_touched(key: &PartitionKey, span: Span) -> i64 {
    match key {
        PartitionKey::Time { granularity, .. } => buckets_between(
            *granularity,
            to_seconds(span.lo, span.unit),
            to_seconds(span.hi, span.unit),
        ),
        PartitionKey::Range {
            start,
            end,
            interval,
            ..
        } => {
            let bucket = |v: i64| (v - start).div_euclid(*interval);
            let inside = |v: i64| v >= *start && v < *end;
            let (lo, hi) = (span.lo.max(*start), span.hi.min(end - 1));
            let within = if lo <= hi {
                bucket(hi) - bucket(lo) + 1
            } else {
                0
            };
            // Values outside the range share one extra partition.
            let outside = i64::from(!inside(span.lo) || !inside(span.hi));
            within + outside
        }
    }
}

/// The time partitions between two Unix seconds, inclusive.
fn buckets_between(granularity: Granularity, lo: i64, hi: i64) -> i64 {
    let (lo, hi) = (lo.min(hi), lo.max(hi));
    match granularity {
        Granularity::Hour => hi.div_euclid(3_600) - lo.div_euclid(3_600) + 1,
        Granularity::Day => hi.div_euclid(86_400) - lo.div_euclid(86_400) + 1,
        Granularity::Month => months_since_epoch(hi) - months_since_epoch(lo) + 1,
        Granularity::Year => i64::from(year_of(hi)) - i64::from(year_of(lo)) + 1,
    }
}

fn year_of(secs: i64) -> i32 {
    DateTime::from_timestamp(secs, 0).map_or(1970, |t| t.year())
}

fn months_since_epoch(secs: i64) -> i64 {
    DateTime::from_timestamp(secs, 0)
        .map_or(0, |t| i64::from(t.year()) * 12 + i64::from(t.month0()))
}

/// `2026-09-01 10:00` for a time value, the plain number for a range one.
fn render(v: i64, unit: Unit) -> String {
    match unit {
        Unit::Plain => v.to_string(),
        unit => DateTime::from_timestamp(to_seconds(v, unit), 0)
            .map_or_else(|| v.to_string(), |t| t.format("%Y-%m-%d %H:%M").to_string()),
    }
}

/// Why the load is refused, naming the coarser granularity that would fit.
fn over_budget_message(key: &PartitionKey, span: Span, touched: i64) -> String {
    let (lo, hi) = (render(span.lo, span.unit), render(span.hi, span.unit));
    match key {
        PartitionKey::Time {
            column,
            granularity,
        } => {
            let column = column.as_deref().unwrap_or("load time");
            let coarser = granularity.coarser().map_or_else(
                || "load a narrower range".to_string(),
                |g| {
                    let n = buckets_between(
                        g,
                        to_seconds(span.lo, span.unit),
                        to_seconds(span.hi, span.unit),
                    );
                    format!(
                        "use `granularity: {}` (about {n}) or load a narrower range",
                        g.as_str()
                    )
                },
            );
            format!(
                "the load spans about {touched} {} partitions of `{column}` ({lo} to {hi}); \
                 BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — {coarser}",
                granularity.as_str()
            )
        }
        PartitionKey::Range {
            column, interval, ..
        } => format!(
            "the load spans about {touched} ranges of `{column}` ({lo} to {hi}, {interval} wide); \
             BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — widen \
             `range.interval` or load a narrower range"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Date32Array, Int64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit as ArrowUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;

    const HOUR: i64 = 3_600;
    const DAY: i64 = 86_400;

    fn micros(secs: i64) -> i64 {
        secs * 1_000_000
    }

    /// Unix seconds of a UTC date.
    fn at(y: i32, m: u32, d: u32, h: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    fn write(dir: &std::path::Path, name: &str, field: Field, column: ArrayRef, stats: bool) {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(if stats {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            })
            .build();
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn ts_file(dir: &std::path::Path, name: &str, secs: &[i64], stats: bool) {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let values: Vec<i64> = secs.iter().map(|s| micros(*s)).collect();
        let column: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        write(dir, name, field, column, stats);
    }

    fn time(column: &str, granularity: Granularity) -> TablePartition {
        TablePartition {
            key: PartitionKey::Time {
                column: Some(column.into()),
                granularity,
            },
            expr: String::new(),
            expiration_days: None,
            require_filter: false,
        }
    }

    fn check(dir: &std::path::Path, names: &[&str], partition: &TablePartition) -> Result<()> {
        let store = GcsStore::open_fs(dir.to_str().unwrap()).unwrap();
        let uris: Vec<String> = names.iter().map(|n| format!("gs://b/{n}")).collect();
        check_partition_budget(&store, &uris, partition)
    }

    #[test]
    fn a_span_within_the_job_cap_passes_and_a_hourly_one_over_it_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        ts_file(dir.path(), "a.parquet", &[start, start + 3 * DAY], true);
        ts_file(
            dir.path(),
            "b.parquet",
            &[start + 5 * DAY, start + 200 * DAY],
            true,
        );
        let files = ["a.parquet", "b.parquet"];
        assert!(check(dir.path(), &files, &time("ts", Granularity::Day)).is_ok());
        let err = check(dir.path(), &files, &time("ts", Granularity::Hour))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "about 4801 hour partitions of `ts` (2026-01-01 00:00 to 2026-07-20 00:00)"
            ),
            "{err}"
        );
        assert!(err.contains("use `granularity: day` (about 201)"), "{err}");
    }

    #[test]
    fn a_file_without_statistics_leaves_the_check_to_bigquery() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        ts_file(dir.path(), "a.parquet", &[start, start + 400 * DAY], false);
        assert!(check(dir.path(), &["a.parquet"], &time("ts", Granularity::Hour)).is_ok());
        assert!(
            check(
                dir.path(),
                &["a.parquet"],
                &time("other", Granularity::Hour)
            )
            .is_ok(),
            "a column the file lacks"
        );
    }

    #[test]
    fn a_load_time_partition_reads_no_footer() {
        let dir = tempfile::tempdir().unwrap();
        let ingestion = TablePartition {
            key: PartitionKey::Time {
                column: None,
                granularity: Granularity::Hour,
            },
            ..time("ts", Granularity::Hour)
        };
        assert!(check(dir.path(), &["missing.parquet"], &ingestion).is_ok());
    }

    #[test]
    fn a_date_column_counts_days_from_its_day_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let field = Field::new("d", DataType::Date32, false);
        let days: Vec<i32> = vec![20_000, 20_000 + 4_500];
        let column: ArrayRef = Arc::new(Date32Array::from(days));
        write(dir.path(), "d.parquet", field, column, true);
        let err = check(dir.path(), &["d.parquet"], &time("d", Granularity::Day))
            .unwrap_err()
            .to_string();
        assert!(err.contains("about 4501 day partitions of `d`"), "{err}");
        assert!(
            err.contains("use `granularity: month` (about 148)"),
            "{err}"
        );
        assert!(check(dir.path(), &["d.parquet"], &time("d", Granularity::Month)).is_ok());
    }

    #[test]
    fn a_range_counts_its_buckets_plus_one_for_values_outside() {
        let dir = tempfile::tempdir().unwrap();
        let field = Field::new("n", DataType::Int64, false);
        let column: ArrayRef = Arc::new(Int64Array::from(vec![-5_i64, 6_000]));
        write(dir.path(), "n.parquet", field, column, true);
        let range = |interval: i64| TablePartition {
            key: PartitionKey::Range {
                column: "n".into(),
                start: 0,
                end: 5_000,
                interval,
            },
            ..time("n", Granularity::Day)
        };
        let err = check(dir.path(), &["n.parquet"], &range(1))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("about 5001 ranges of `n` (-5 to 6000, 1 wide)"),
            "{err}"
        );
        assert!(err.contains("widen `range.interval`"), "{err}");
        assert!(check(dir.path(), &["n.parquet"], &range(2)).is_ok());
    }

    #[test]
    fn buckets_count_calendar_boundaries_not_elapsed_time() {
        let (lo, hi) = (at(2025, 12, 31, 23), at(2026, 1, 1, 0));
        assert_eq!(buckets_between(Granularity::Hour, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Day, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Month, lo, hi), 2);
        assert_eq!(buckets_between(Granularity::Year, lo, hi), 2);
        let (lo, hi) = (at(2026, 1, 31, 5), at(2026, 3, 1, 0));
        assert_eq!(buckets_between(Granularity::Month, lo, hi), 3);
        assert_eq!(buckets_between(Granularity::Year, lo, hi), 1);
        assert_eq!(buckets_between(Granularity::Day, hi, lo), 30, "order-free");
        assert_eq!(buckets_between(Granularity::Hour, 0, HOUR - 1), 1);
    }

    #[test]
    fn stored_units_follow_the_parquet_type() {
        assert_eq!(
            stored_unit(Some(&LogicalType::Date), PhysicalType::INT32),
            Some(Unit::Days)
        );
        let ts = |unit| {
            LogicalType::Timestamp(TimestampType {
                is_adjusted_to_u_t_c: true,
                unit,
            })
        };
        assert_eq!(
            stored_unit(Some(&ts(TimeUnit::MILLIS)), PhysicalType::INT64),
            Some(Unit::Millis)
        );
        assert_eq!(
            stored_unit(Some(&ts(TimeUnit::NANOS)), PhysicalType::INT64),
            Some(Unit::Nanos)
        );
        assert_eq!(stored_unit(None, PhysicalType::INT64), Some(Unit::Plain));
        assert_eq!(stored_unit(None, PhysicalType::BYTE_ARRAY), None);
        assert_eq!(
            stored_unit(Some(&LogicalType::String), PhysicalType::BYTE_ARRAY),
            None
        );
        assert_eq!(to_seconds(2, Unit::Days), 2 * DAY);
        assert_eq!(
            to_seconds(-1, Unit::Millis),
            -1,
            "floors toward minus infinity"
        );
        assert_eq!(to_seconds(1_500_000_000, Unit::Nanos), 1);
    }
}
