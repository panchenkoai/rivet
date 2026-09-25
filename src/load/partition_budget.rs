//! The partitions a load touches, estimated from the Parquet footers it is about to
//! load — before any BigQuery job runs (ADR-0034 D4). BigQuery writes at most 4,000
//! partitions per job; a finer granularity than the data's span allows fails only after
//! the free load job ran, with no way to split the load from rivet. Footer statistics
//! give the partition column's range per file for two small range reads each.

use crate::destination::gcs::GcsStore;
use crate::load;
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

/// One file to pack: its span, its rows, and whether any value is NULL — a partition of
/// its own that `min`/`max` cannot see.
struct Part {
    uri: String,
    span: Span,
    rows: i64,
    has_nulls: bool,
}

/// The partitions a file or batch writes: its span's buckets, one more for the NULL
/// partition, and never more than its rows.
fn budgeted(key: &PartitionKey, span: Span, rows: i64, has_nulls: bool) -> i64 {
    (partitions_touched(key, span) + i64::from(has_nulls)).min(rows)
}

/// Refuse a load one of whose files alone spans more partitions than one BigQuery job
/// may write — the shape no batching can split. Everything else is loadable in batches
/// (see [`plan_load_batches`]).
pub(crate) fn check_partition_budget(
    store: &GcsStore,
    uris: &[String],
    partition: &TablePartition,
) -> Result<()> {
    plan_load_batches(store, uris, partition).map(|_| ())
}

/// The load jobs this Parquet needs: files packed, in order of their lowest value, into
/// batches whose combined span stays within BigQuery's per-job partition cap. Files are
/// date-local when the extraction key grows with time (an autoincrement key over a
/// `created_at` history), so a history far wider than one job may write still loads
/// under its declared granularity. A file without statistics for the column rides in
/// the first batch — BigQuery is its backstop, as before. One file wider than the cap
/// is refused, by name: nothing splits it.
pub(crate) fn plan_load_batches(
    store: &GcsStore,
    uris: &[String],
    partition: &TablePartition,
) -> Result<Vec<Vec<String>>> {
    let Some(column) = partition.key.column() else {
        return Ok(vec![uris.to_vec()]);
    };
    let mut spanned: Vec<Part> = Vec::new();
    let mut blind: Vec<String> = Vec::new();
    for uri in uris {
        let (_, key) = crate::load::split_gs_uri(uri)?;
        let meta = read_footer(store, key)
            .with_context(|| format!("reading the Parquet footer of {uri}"))?;
        let rows = meta.file_metadata().num_rows();
        // A part the writer budgeted says how many partitions it holds — the bound the
        // span cannot give for a scattered part (4,000 distinct days across 5,600).
        let rows = footer_buckets(&meta, &partition.key).map_or(rows, |b| rows.min(b));
        match column_span(&meta, column) {
            Some((span, has_nulls)) => spanned.push(Part {
                uri: uri.clone(),
                span,
                rows,
                has_nulls,
            }),
            None => blind.push(uri.clone()),
        }
    }
    let mut batches = pack_batches(&partition.key, spanned)?;
    if !blind.is_empty() {
        match batches.first_mut() {
            Some(first) => first.extend(blind),
            None => batches.push(blind),
        }
    }
    Ok(batches)
}

/// Pure packing: sort by the low end, then extend the current batch while the partitions
/// it would write stay within the cap. Refuses a single file over the cap, naming it.
///
/// A file's partitions are bounded BOTH by the span of its values and by its ROW COUNT —
/// N rows cannot occupy more than N partitions, whatever they span. The footer carries
/// min/max but no distinct count, so the span alone badly over-counts a scattered file:
/// measured on a live export, a 1,001-row part spanning 9,758 days occupies exactly 1,001
/// partitions and BigQuery loaded it, while this check refused it by name.
///
/// BOTH the refusal and the packing take the smaller bound. The refusal always did,
/// because that is where a wrong answer costs the operator a load they cannot perform.
/// The packing used to stay on the span alone, on the reasoning that being conservative
/// there "can only cost one more load job than strictly needed" — which is false for the
/// shape this same file documents as NORMAL below: an incremental export orders rows by
/// its cursor, so its parts are each scattered across the whole history, every pairwise
/// merge busts the span cap, and each part becomes its own load job. The cost grows with
/// the PART COUNT, not by one. Taking `min(rows)` here can only merge MORE, never refuse,
/// so it cannot turn a loadable batch into a rejection.
fn pack_batches(key: &PartitionKey, mut files: Vec<Part>) -> Result<Vec<Vec<String>>> {
    files.sort_by_key(|p| p.span.lo);
    let mut batches: Vec<Vec<String>> = Vec::new();
    // The batch under construction carries the same three facts as a file, because the
    // merge decision below applies the same bound as the per-file check above.
    let mut current: Option<(Vec<String>, Span, i64, bool)> = None;
    for Part {
        uri,
        span,
        rows,
        has_nulls,
    } in files
    {
        let alone = budgeted(key, span, rows, has_nulls);
        if alone > MAX_PARTITIONS_PER_JOB {
            bail!(
                "{uri} alone {} — no batching splits one file",
                over_budget_message(key, span, alone)
            );
        }
        current = Some(match current {
            None => (vec![uri], span, rows, has_nulls),
            Some((mut uris, cur, cur_rows, cur_nulls)) => {
                let merged = merge(cur, span);
                let merged_rows = cur_rows.saturating_add(rows);
                let merged_nulls = cur_nulls || has_nulls;
                if budgeted(key, merged, merged_rows, merged_nulls) > MAX_PARTITIONS_PER_JOB {
                    batches.push(uris);
                    (vec![uri], span, rows, has_nulls)
                } else {
                    uris.push(uri);
                    (uris, merged, merged_rows, merged_nulls)
                }
            }
        });
    }
    if let Some((uris, ..)) = current {
        batches.push(uris);
    }
    Ok(batches)
}

/// The distinct partitions the writer recorded for this file, when it budgeted the same
/// column at the same granularity the load partitions by.
fn footer_buckets(meta: &ParquetMetaData, key: &PartitionKey) -> Option<i64> {
    use crate::plan::rollover::{PARTITION_BUCKETS_KEY, parse_partition_buckets_note};
    let PartitionKey::Time {
        column: Some(column),
        granularity,
    } = key
    else {
        return None;
    };
    let note = meta
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == PARTITION_BUCKETS_KEY)?
        .value
        .as_deref()?;
    let (c, g, n) = parse_partition_buckets_note(note)?;
    (c == column && g == granularity.as_str()).then_some(n)
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
fn column_span(meta: &ParquetMetaData, column: &str) -> Option<(Span, bool)> {
    let schema = meta.file_metadata().schema_descr();
    let idx = schema.columns().iter().position(|c| c.name() == column)?;
    let descr = schema.column(idx);
    let unit = stored_unit(descr.logical_type_ref(), descr.physical_type())?;
    let mut span: Option<Span> = None;
    // NULL rows occupy a partition of their own, and `min`/`max` exclude them by
    // definition — so the span alone can never see it. Only a KNOWN non-zero count
    // sets the bit: parquet's own docs warn that a missing null-count statistic
    // means UNKNOWN (writers before 53.1.0 omitted it when the count was zero), and
    // treating unknown as "has nulls" would inflate the count toward a REFUSAL of a
    // load that is perfectly performable. This module's rule is that the refusal
    // takes the smaller bound, because a wrong answer there costs the operator a
    // load they cannot make; being cautious the other way costs at most a job.
    let mut nulls = false;
    for rg in meta.row_groups() {
        let stats = rg.column(idx).statistics()?;
        nulls |= stats.null_count_opt().is_some_and(|n| n > 0);
        let (lo, hi) = match stats {
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
    span.map(|s| (s, nulls))
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
        (None, PhysicalType::INT64 | PhysicalType::INT32)
        | (Some(LogicalType::Integer { .. }), PhysicalType::INT64 | PhysicalType::INT32) => {
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
                "spans about {touched} {} partitions of `{column}` ({lo} to {hi}); \
                 BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — {coarser}",
                granularity.as_str()
            )
        }
        PartitionKey::Range {
            column, interval, ..
        } => format!(
            "spans about {touched} ranges of `{column}` ({lo} to {hi}, {interval} wide); \
             BigQuery writes at most {MAX_PARTITIONS_PER_JOB} partitions per job — widen \
             `range.interval` or load a narrower range"
        ),
    }
}

/// The URIs the partition budget applies to: the files that land in the
/// PARTITIONED target. Under `BaseAndBuffer` the stream's files land in the
/// BUFFER, which is created WITHOUT a partition and read whole by one MERGE, so
/// budgeting them against the base's granularity refuses a load that would have
/// worked. Found by dogfooding (2026-09-18): a 5,000-day buffer file on a
/// day-partitioned base was refused by name, although no job would ever write
/// those partitions — the adapter had already stopped packing the buffer, but
/// this preflight still measured it.
///
/// A baseline manifest that resolves to no present part makes `select_load_keys`
/// fall back to the whole listing; the check then covers everything again, which
/// is the conservative direction.
pub(crate) fn budgeted_uris(
    layout: load::plan::CdcLayout,
    runs: &[(String, crate::manifest::RunManifest)],
    uris: &[String],
) -> Vec<String> {
    if !layout.log_is_disposable() {
        return uris.to_vec();
    }
    let baseline: Vec<(String, crate::manifest::RunManifest)> = runs
        .iter()
        .filter(|(_, m)| load::orchestrate::is_baseline_leg(m))
        .cloned()
        .collect();
    if baseline.is_empty() {
        return Vec::new();
    }
    let keys: Vec<String> = uris
        .iter()
        .filter_map(|u| load::split_gs_uri(u).ok().map(|(_, k)| k.to_string()))
        .collect();
    let want: std::collections::HashSet<String> =
        load::reconcile::select_load_keys(&baseline, &keys)
            .into_iter()
            .collect();
    uris.iter()
        .filter(|u| load::split_gs_uri(u).is_ok_and(|(_, k)| want.contains(k)))
        .cloned()
        .collect()
}

/// The pre-load partition budget of a BigQuery plan (ADR-0034 D4); no other target
/// caps the partitions one job writes.
pub(crate) fn partition_budget_ok(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    uris: &[String],
) -> Result<()> {
    match (&plan.load.target, &plan.partition) {
        (load::plan::LoadTarget::Bigquery { .. }, Some(partition)) => {
            load::partition_budget::check_partition_budget(store, uris, partition)
                .with_context(|| format!("export `{}`", plan.export_name))
        }
        _ => Ok(()),
    }
}

/// The partition a load declares, for the progress line.
pub(crate) fn partition_label(plan: &load::plan::LoadPlan) -> String {
    plan.partition
        .as_ref()
        .map_or_else(|| "none".to_string(), |p| p.key.describe())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
    };
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
        write_noted(dir, name, field, column, stats, None);
    }

    #[test]
    fn the_budget_applies_only_to_a_partitioned_bigquery_plan_and_names_the_export() {
        use crate::load::plan::{LoadMode, test_plan};
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let days: Vec<i64> = (0..4100).map(|i| start + i * DAY).collect();
        let (field, column) = ts_column(&days);
        write_noted(dir.path(), "wide.parquet", field, column, true, None);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris = vec!["gs://b/wide.parquet".to_string()];

        let mut plan = test_plan(LoadMode::Full, "gs://b/");
        assert_eq!(super::partition_label(&plan), "none");
        assert!(
            super::partition_budget_ok(&store, &plan, &uris).is_ok(),
            "no partition, no cap"
        );

        plan.partition = Some(time("ts", Granularity::Day));
        assert_eq!(
            super::partition_label(&plan),
            plan.partition.as_ref().unwrap().key.describe()
        );
        let err = super::partition_budget_ok(&store, &plan, &uris).unwrap_err();
        assert!(format!("{err:#}").contains("export `orders`"), "{err:#}");
    }

    fn write_noted(
        dir: &std::path::Path,
        name: &str,
        field: Field,
        column: ArrayRef,
        stats: bool,
        note: Option<&str>,
    ) {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(if stats {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            })
            .set_key_value_metadata(note.map(|n| {
                vec![parquet::file::metadata::KeyValue::new(
                    crate::plan::rollover::PARTITION_BUCKETS_KEY.to_string(),
                    n.to_string(),
                )]
            }))
            .build();
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn ts_column(secs: &[i64]) -> (Field, ArrayRef) {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let values: Vec<i64> = secs.iter().map(|s| micros(*s)).collect();
        let column: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        (field, column)
    }

    fn ts_file(dir: &std::path::Path, name: &str, secs: &[i64], stats: bool) {
        let (field, column) = ts_column(secs);
        write(dir, name, field, column, stats);
    }

    /// The writer rotates parts at 4,000 DISTINCT partitions, and a part cut there over
    /// a gappy history spans far more calendar days than it occupies — 4,000 business
    /// days are ~5,600 calendar days. Its footer min/max, and its row count once it is
    /// large, both say "over the cap"; the count the writer recorded in the footer is
    /// the truth, and only for the column and granularity the load partitions by.
    /// RED against ignoring the footer note.
    #[test]
    fn a_part_the_writer_budgeted_is_bounded_by_the_count_it_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        // 4,000 distinct days two days apart (an 8,000-day span), 100 of them twice.
        let mut days: Vec<i64> = (0..4000).map(|i| start + i * 2 * DAY).collect();
        days.extend((0..100).map(|i| start + i * 2 * DAY));
        let key = time("ts", Granularity::Day);
        let uri = |n: &str| vec![format!("gs://b/{n}")];

        let (field, column) = ts_column(&days);
        write_noted(dir.path(), "bare.parquet", field, column, true, None);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let err = plan_load_batches(&store, &uri("bare.parquet"), &key)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("alone spans about 4100 day partitions"),
            "without the note the rows (4,100) are the tighter bound, and still over: {err}"
        );

        let (field, column) = ts_column(&days);
        write_noted(
            dir.path(),
            "noted.parquet",
            field,
            column,
            true,
            Some("ts|day|4000"),
        );
        assert_eq!(
            plan_load_batches(&store, &uri("noted.parquet"), &key).unwrap(),
            vec![uri("noted.parquet")],
            "4,000 recorded partitions fit one job, whatever the span"
        );

        // A note about another column, or another granularity, says nothing about
        // this load and must not be trusted.
        for foreign in ["other|day|4000", "ts|month|4000"] {
            let (field, column) = ts_column(&days);
            write_noted(
                dir.path(),
                "foreign.parquet",
                field,
                column,
                true,
                Some(foreign),
            );
            assert!(
                plan_load_batches(&store, &uri("foreign.parquet"), &key).is_err(),
                "a note for {foreign} must not bound a DAY load of `ts`"
            );
        }
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
        // One row per hour across the span: the row count must not be the binding bound
        // here, or this would assert the refusal for the wrong reason (a two-row file
        // cannot occupy 4,681 partitions and is accepted on its row count alone).
        let hourly: Vec<i64> = (0..=4680).map(|h| start + 5 * DAY + h * HOUR).collect();
        ts_file(dir.path(), "b.parquet", &hourly, true);
        let files = ["a.parquet", "b.parquet"];
        assert!(check(dir.path(), &files, &time("ts", Granularity::Day)).is_ok());
        // `b` ALONE spans 4681 hours: no batching splits one file, so it is named.
        let err = check(dir.path(), &files, &time("ts", Granularity::Hour))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("gs://b/b.parquet alone spans about 4681 hour partitions of `ts`"),
            "{err}"
        );
        assert!(
            err.contains("(2026-01-06 00:00 to 2026-07-20 00:00)"),
            "{err}"
        );
        assert!(err.contains("use `granularity: day` (about 196)"), "{err}");
    }

    /// The cap is inclusive: a file (or a batch) touching EXACTLY 4,000 partitions is
    /// one job; one more partition splits or refuses.
    #[test]
    fn exactly_the_cap_is_one_job_and_one_more_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        // One row per day, so the row count is never the binding bound and the boundary
        // under test is the partition count itself.
        let daily = |n: i64| -> Vec<i64> { (0..n).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "cap.parquet", &daily(4000), true);
        ts_file(dir.path(), "over.parquet", &daily(4001), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let key = time("ts", Granularity::Day);
        assert_eq!(
            plan_load_batches(&store, &["gs://b/cap.parquet".to_string()], &key).unwrap(),
            vec![vec!["gs://b/cap.parquet".to_string()]],
            "4,000 day partitions fit one job"
        );
        let err = plan_load_batches(&store, &["gs://b/over.parquet".to_string()], &key)
            .unwrap_err()
            .to_string();
        assert!(err.contains("alone spans about 4001"), "{err}");
    }

    fn ts_nullable(dir: &std::path::Path, name: &str, secs: &[Option<i64>]) {
        let field = Field::new(
            "ts",
            DataType::Timestamp(ArrowUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let values: Vec<Option<i64>> = secs.iter().map(|s| s.map(micros)).collect();
        let column: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
        write(dir, name, field, column, true);
    }

    /// NULL values land in a partition of their own, which `min`/`max` cannot see: a
    /// dense 4,000-day file with a few NULL rows writes 4,001 partitions, and so do two
    /// 2,000-day files when one of them carries a NULL. The writer's budget counts that
    /// partition when it cuts a part; the load must count it too, or it admits a job
    /// BigQuery rejects after the load ran. A nullable column holding NO null is not
    /// charged — parquet records the count as `Some(0)`, and only a known non-zero
    /// count sets the bit.
    /// RED against dropping the bit at either site (accepted as 4,000 / one batch).
    #[test]
    fn a_null_value_occupies_a_partition_of_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        let daily = |from: i64, to: i64| -> Vec<Option<i64>> {
            (from..to).map(|d| Some(start + d * DAY)).collect()
        };
        let key = time("ts", Granularity::Day);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris = |names: &[&str]| -> Vec<String> {
            names.iter().map(|n| format!("gs://b/{n}")).collect()
        };

        let mut with_nulls = daily(0, 4000);
        with_nulls.extend([None; 5]);
        ts_nullable(dir.path(), "nulls.parquet", &with_nulls);
        let err = plan_load_batches(&store, &uris(&["nulls.parquet"]), &key)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("alone spans about 4001 day partitions"),
            "4,000 days plus the NULL partition: {err}"
        );

        // One day twice, so the ROW clamp (4,001) is not what holds this at 4,000: a
        // `Some(0)` charged a partition would read 4,001 and refuse. With exactly 4,000
        // rows the clamp hid that mutant — measured, not reasoned.
        let mut clean = daily(0, 4000);
        clean.push(Some(start));
        ts_nullable(dir.path(), "clean.parquet", &clean);
        assert_eq!(
            plan_load_batches(&store, &uris(&["clean.parquet"]), &key).unwrap(),
            vec![uris(&["clean.parquet"])],
            "a nullable column holding no NULL is not charged a partition"
        );

        let mut a = daily(0, 2000);
        a.extend([None; 3]);
        ts_nullable(dir.path(), "a.parquet", &a);
        ts_nullable(dir.path(), "b.parquet", &daily(2000, 4000));
        assert_eq!(
            plan_load_batches(&store, &uris(&["a.parquet", "b.parquet"]), &key).unwrap(),
            vec![uris(&["a.parquet"]), uris(&["b.parquet"])],
            "4,000 days plus the NULL partition is 4,001: two jobs, not one"
        );
    }

    /// A file whose values are SCATTERED over a huge range occupies one partition per
    /// distinct value, not one per day of its range — and a file of N rows can never
    /// occupy more than N partitions. Refusing it on the span alone rejects a file the
    /// warehouse loads happily.
    ///
    /// Measured 2026-09-18 on a live export: a 1,001-row part spanning 9,758 days was
    /// refused here, while `bq load` of the same file into a DAY-partitioned table
    /// succeeded and `INFORMATION_SCHEMA.PARTITIONS` reported exactly 1,001 partitions.
    /// An incremental export orders rows by its CURSOR, so its parts are scattered over
    /// the date column by construction — this is the normal shape, not a corner case.
    ///
    /// RED against taking the span alone: "alone spans about 9758 day partitions".
    #[test]
    fn a_scattered_file_is_bounded_by_its_rows_not_its_span() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        // 1,001 rows spread every ~9 days across 26 years: a 9,758-day span, 1,001
        // partitions. Under the cap by rows, far over it by span.
        let scattered: Vec<i64> = (0..1001).map(|i| start + i * 9 * DAY).collect();
        ts_file(dir.path(), "scattered.parquet", &scattered, true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let key = time("ts", Granularity::Day);
        assert_eq!(
            plan_load_batches(&store, &["gs://b/scattered.parquet".to_string()], &key).unwrap(),
            vec![vec!["gs://b/scattered.parquet".to_string()]],
            "1,001 rows can touch at most 1,001 partitions, whatever they span"
        );
    }

    /// Files that are each narrow but together wide load in BATCHES: packed by their
    /// low end, a batch closed the moment the next file would push its span past the
    /// cap. Three files over 6000 days → two jobs, never one refusal.
    #[test]
    fn narrow_files_over_a_wide_history_are_packed_into_batches_under_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        // Written out of order on purpose: packing sorts by the low end.
        // DENSE, one row per day, so the SPAN is the binding bound and this test
        // still measures span packing. With the old two-row files the row bound
        // (`min(rows)`, added when packing stopped over-counting scattered parts)
        // dominated completely and everything merged into one job — the fixture
        // would have stopped crossing the threshold it exists to cross.
        let days =
            |from: i64, to: i64| -> Vec<i64> { (from..=to).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "c.parquet", &days(4200, 6000), true);
        ts_file(dir.path(), "a.parquet", &days(0, 2000), true);
        ts_file(dir.path(), "b.parquet", &days(2001, 3999), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris: Vec<String> = ["c.parquet", "a.parquet", "b.parquet"]
            .iter()
            .map(|n| format!("gs://b/{n}"))
            .collect();
        let batches = plan_load_batches(&store, &uris, &time("ts", Granularity::Day)).unwrap();
        assert_eq!(
            batches,
            vec![
                vec![
                    "gs://b/a.parquet".to_string(),
                    "gs://b/b.parquet".to_string()
                ],
                vec!["gs://b/c.parquet".to_string()],
            ],
            "a+b span exactly 4000 days and fit one job; c starts the next"
        );
        // A month granularity fits everything in one job.
        let one = plan_load_batches(&store, &uris, &time("ts", Granularity::Month)).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].len(), 3);
    }

    /// A file with no statistics cannot be placed; it rides in the first batch and
    /// BigQuery remains its backstop — it is never dropped from the load.
    #[test]
    fn a_stats_less_file_rides_in_the_first_batch() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2000, 1, 1, 0);
        // Dense for the same reason as the packing test above: two-row files let the
        // row bound decide, and then a+z merge into ONE batch and the blind file has
        // no second batch to be absent from. The subject here is that the blind file
        // is never DROPPED, which needs a real two-batch split to be worth asserting.
        let days =
            |from: i64, to: i64| -> Vec<i64> { (from..=to).map(|d| start + d * DAY).collect() };
        ts_file(dir.path(), "a.parquet", &days(0, 2099), true);
        ts_file(dir.path(), "blind.parquet", &days(0, 9000), false);
        ts_file(dir.path(), "z.parquet", &days(5000, 7099), true);
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        let uris: Vec<String> = ["a.parquet", "blind.parquet", "z.parquet"]
            .iter()
            .map(|n| format!("gs://b/{n}"))
            .collect();
        let batches = plan_load_batches(&store, &uris, &time("ts", Granularity::Day)).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(
            batches[0].contains(&"gs://b/blind.parquet".to_string()),
            "{batches:?}"
        );
        assert_eq!(
            batches.concat().len(),
            3,
            "every file is loaded exactly once"
        );
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
        // One row per day across the span: the subject here is the day-number arithmetic,
        // so the ROW COUNT must not be the binding bound (a two-row file can occupy at
        // most two partitions and is accepted on that alone).
        let days: Vec<i32> = (20_000..=20_000 + 4_500).collect();
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
        // The bounds under test, padded to more rows than the cap so the row count is not
        // the binding bound — the subject is the range-bucket arithmetic.
        let mut values: Vec<i64> = vec![-5_i64, 6_000];
        values.extend(0..5_000);
        let column: ArrayRef = Arc::new(Int64Array::from(values));
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

    /// A narrower integer column (PG `integer`, MySQL `int`) is stored as INT32 with the
    /// same statistics; the range budget reads it like INT64.
    #[test]
    fn a_range_on_an_int32_column_is_budgeted_like_int64() {
        let dir = tempfile::tempdir().unwrap();
        // Padded past the cap for the same reason as the INT64 case: the row count must
        // not stand in for the bucket arithmetic this test is about.
        let mut i32_values: Vec<i32> = vec![-5_i32, 6_000];
        i32_values.extend(0..5_000);
        let i32_col: ArrayRef = Arc::new(Int32Array::from(i32_values));
        write(
            dir.path(),
            "i32.parquet",
            Field::new("n", DataType::Int32, false),
            i32_col,
            true,
        );
        let mut i16_values: Vec<i16> = vec![-5_i16, 6_000];
        i16_values.extend(0..5_000);
        let i16_col: ArrayRef = Arc::new(Int16Array::from(i16_values));
        write(
            dir.path(),
            "i16.parquet",
            Field::new("n", DataType::Int16, false),
            i16_col,
            true,
        );
        let range = |interval: i64| TablePartition {
            key: PartitionKey::Range {
                column: "n".into(),
                start: 0,
                end: 5_000,
                interval,
            },
            ..time("n", Granularity::Day)
        };
        for file in ["i32.parquet", "i16.parquet"] {
            let err = check(dir.path(), &[file], &range(1))
                .unwrap_err()
                .to_string();
            assert!(err.contains("about 5001 ranges of `n`"), "{file}: {err}");
            assert!(check(dir.path(), &[file], &range(2)).is_ok(), "{file}");
        }
    }

    /// One part without statistics leaves the check to BigQuery for ITS rows only: the
    /// span the other parts prove is still refused, whichever order the parts come in.
    #[test]
    fn a_stats_less_file_does_not_hide_the_other_files_span() {
        let dir = tempfile::tempdir().unwrap();
        let start = at(2026, 1, 1, 0);
        // One row per hour over the span, so `a`'s refusal rests on its 4,801 partitions
        // rather than on a two-row file that could never occupy them.
        let hourly: Vec<i64> = (0..=4800).map(|h| start + h * HOUR).collect();
        ts_file(dir.path(), "a.parquet", &hourly, true);
        ts_file(dir.path(), "b.parquet", &[start], false);
        for files in [["a.parquet", "b.parquet"], ["b.parquet", "a.parquet"]] {
            let err = check(dir.path(), &files, &time("ts", Granularity::Hour))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("about 4801 hour partitions"),
                "{files:?}: {err}"
            );
        }
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
