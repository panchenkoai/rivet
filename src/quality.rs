// Functions in this module are called from pipeline::sink, pipeline::mod, and integration tests
// via the library crate. The binary re-declares this module but does not call all functions
// directly, producing dead_code warnings only for the bin target.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::QualityConfig;
use crate::error::Result;

/// Hash a single non-null array value using typed dispatch to avoid string formatting.
/// Returns `None` for null values; caller skips those in uniqueness tracking.
fn hash_value(array: &dyn Array, row: usize) -> Option<u64> {
    if array.is_null(row) {
        return None;
    }
    let h = match array.data_type() {
        DataType::Boolean => {
            let v = array.as_any().downcast_ref::<BooleanArray>()?.value(row);
            xxh3_64(&[v as u8])
        }
        DataType::Int8 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Int8Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Int16 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Int16Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Int32 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Int64 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::UInt8 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<UInt8Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::UInt16 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<UInt16Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::UInt32 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<UInt32Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::UInt64 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Float32 => {
            let bits = array
                .as_any()
                .downcast_ref::<Float32Array>()?
                .value(row)
                .to_bits();
            xxh3_64(&bits.to_le_bytes())
        }
        DataType::Float64 => {
            let bits = array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(row)
                .to_bits();
            xxh3_64(&bits.to_le_bytes())
        }
        DataType::Decimal128(_, _) => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Decimal128Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Date32 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Date64 => xxh3_64(
            &array
                .as_any()
                .downcast_ref::<Date64Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Utf8 => xxh3_64(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row)
                .as_bytes(),
        ),
        DataType::LargeUtf8 => xxh3_64(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row)
                .as_bytes(),
        ),
        DataType::Binary => xxh3_64(array.as_any().downcast_ref::<BinaryArray>()?.value(row)),
        DataType::LargeBinary => xxh3_64(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()?
                .value(row),
        ),
        _ => {
            // Fallback for exotic types (Timestamp variants, Duration, etc.)
            let options = arrow::util::display::FormatOptions::default();
            let fmt = arrow::util::display::ArrayFormatter::try_new(array, &options).ok()?;
            xxh3_64(fmt.value(row).to_string().as_bytes())
        }
    };
    Some(h)
}

#[derive(Debug, Clone)]
pub struct QualityIssue {
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
    Warn,
    Fail,
}

/// The single home for the operator-facing quality-gate *failure contract*: the
/// "N check(s) failed" body, the per-issue bullet layout, and the remediation
/// hint. Both the single-export gate (`pipeline::single`) and the chunked
/// aggregate gate (`pipeline::job`) format failures through here so the message
/// — which is a de-facto public contract — cannot drift between modes.
///
/// `context` tags the gate when it matters (e.g. `Some("chunked aggregate")`,
/// where only row-count bounds are checked); `None` for the full per-export gate.
pub fn failure_message(export_name: &str, context: Option<&str>, failing: &[&str]) -> String {
    let ctx = context.map(|c| format!(" ({c})")).unwrap_or_default();
    format!(
        "export '{}': {} quality check(s) failed{}:\n  - {}\n  \
         Fix the source data, or adjust the thresholds under `quality:` in your config.",
        export_name,
        failing.len(),
        ctx,
        failing.join("\n  - "),
    )
}

/// Pre-flight gate (#33, column-applicability): every column named by a quality
/// rule must be produced by the export, or the gate is a silent no-op — the
/// uniqueness loop `index_of(col)` skips a missing column (0 duplicates, "pass")
/// and the null-ratio loop's `unwrap_or(0)` treats a missing column as 0 nulls
/// ("pass"). Per CLAUDE.md ("never a silent no-op"), a quality rule that can
/// never evaluate is a configuration error, not a pass.
///
/// `available` is the set of column names the export actually produces (the
/// output schema). Returns a loud error naming the offending column and the
/// available columns. Call this once the schema is known — before the gate runs
/// — so the run fails fast instead of reporting `quality: pass` over a rule that
/// never fired.
pub fn validate_quality_columns(config: &QualityConfig, available: &[String]) -> Result<()> {
    let mut missing: Vec<&str> = Vec::new();
    for col in &config.unique_columns {
        if !available.iter().any(|c| c == col) {
            missing.push(col.as_str());
        }
    }
    for col in config.null_ratio_max.keys() {
        if !available.iter().any(|c| c == col) && !missing.contains(&col.as_str()) {
            missing.push(col.as_str());
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    anyhow::bail!(
        "quality check references column(s) not produced by the export: {}. \
         Available columns: {}. \
         Fix the column name(s) under `quality:` or add them to the query.",
        missing.join(", "),
        if available.is_empty() {
            "<none>".to_string()
        } else {
            available.join(", ")
        },
    );
}

/// Union of column names across `batches` (the schema the export produced).
/// Used to validate quality-rule column references against reality.
fn available_columns(batches: &[RecordBatch]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for batch in batches {
        for field in batch.schema().fields() {
            if !seen.iter().any(|c| c == field.name()) {
                seen.push(field.name().clone());
            }
        }
    }
    seen
}

pub fn check_row_count(actual: usize, config: &QualityConfig) -> Vec<QualityIssue> {
    let mut issues = Vec::new();
    if let Some(min) = config.row_count_min
        && actual < min
    {
        issues.push(QualityIssue {
            severity: Severity::Fail,
            message: format!("row_count {} below minimum {}", actual, min),
        });
    }
    if let Some(max) = config.row_count_max
        && actual > max
    {
        issues.push(QualityIssue {
            severity: Severity::Fail,
            message: format!("row_count {} exceeds maximum {}", actual, max),
        });
    }
    issues
}

pub fn check_null_ratios(
    batches: &[RecordBatch],
    thresholds: &HashMap<String, f64>,
) -> Vec<QualityIssue> {
    if thresholds.is_empty() {
        return Vec::new();
    }

    // #33: a threshold naming a column the export does not produce can never
    // evaluate — `unwrap_or(0)` would treat it as 0 nulls and silently "pass".
    // Surface it as a Fail (naming the available columns) instead, regardless of
    // row count, so the gate cannot vanish.
    let available = available_columns(batches);
    let mut issues = Vec::new();
    for col_name in thresholds.keys() {
        if !available.iter().any(|c| c == col_name) {
            issues.push(QualityIssue {
                severity: Severity::Fail,
                message: format!(
                    "column '{}': null-ratio check references a column not produced by the \
                     export (available: {}); fix the column name or add it to the query",
                    col_name,
                    if available.is_empty() {
                        "<none>".to_string()
                    } else {
                        available.join(", ")
                    },
                ),
            });
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return issues;
    }

    let mut null_counts: HashMap<String, usize> = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            if thresholds.contains_key(field.name().as_str()) {
                let col = batch.column(i);
                *null_counts.entry(field.name().clone()).or_default() += col.null_count();
            }
        }
    }

    for (col_name, max_ratio) in thresholds {
        // Missing columns already produced a Fail above; skip the ratio math so
        // they don't also yield a spurious "ratio 0.0000 exceeds" line.
        if !available.iter().any(|c| c == col_name) {
            continue;
        }
        let nulls = null_counts.get(col_name.as_str()).copied().unwrap_or(0);
        let ratio = nulls as f64 / total_rows as f64;
        if ratio > *max_ratio {
            issues.push(QualityIssue {
                severity: Severity::Fail,
                message: format!(
                    "column '{}': null ratio {:.4} exceeds threshold {:.4}",
                    col_name, ratio, max_ratio
                ),
            });
        }
    }
    issues
}

pub fn check_uniqueness(
    batches: &[RecordBatch],
    columns: &[String],
    max_entries: Option<usize>,
) -> Vec<QualityIssue> {
    if columns.is_empty() {
        return Vec::new();
    }

    // #33: a uniqueness column the export does not produce can never evaluate —
    // `index_of(col)` returns `Err`, the inner loop is skipped, 0 duplicates are
    // counted and the gate silently "passes". Surface it as a Fail (naming the
    // available columns) instead, regardless of row count, so the gate cannot
    // vanish (CLAUDE.md: never a silent no-op).
    let available = available_columns(batches);
    let mut issues = Vec::new();
    for col_name in columns {
        if !available.iter().any(|c| c == col_name) {
            issues.push(QualityIssue {
                severity: Severity::Fail,
                message: format!(
                    "column '{}': uniqueness check references a column not produced by the \
                     export (available: {}); fix the column name or add it to the query",
                    col_name,
                    if available.is_empty() {
                        "<none>".to_string()
                    } else {
                        available.join(", ")
                    },
                ),
            });
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return issues;
    }

    for col_name in columns {
        if !available.iter().any(|c| c == col_name) {
            continue; // already reported as a Fail above
        }
        let mut seen: HashSet<u64> = HashSet::new();
        let mut duplicates = 0usize;
        let mut capped = false;

        'batches: for batch in batches {
            if let Ok(idx) = batch.schema().index_of(col_name) {
                let col = batch.column(idx);
                for row in 0..col.len() {
                    if max_entries.is_some_and(|limit| seen.len() >= limit) {
                        capped = true;
                        break 'batches;
                    }
                    if let Some(h) = hash_value(col.as_ref(), row)
                        && !seen.insert(h)
                    {
                        duplicates += 1;
                    }
                }
            }
        }

        if capped {
            issues.push(QualityIssue {
                severity: Severity::Warn,
                message: format!(
                    "column '{}': uniqueness check capped at {} entries; result may be incomplete",
                    col_name,
                    max_entries.unwrap_or(0)
                ),
            });
        }

        if duplicates > 0 {
            issues.push(QualityIssue {
                severity: Severity::Fail,
                message: format!(
                    "column '{}': {} duplicate values out of {} rows",
                    col_name, duplicates, total_rows
                ),
            });
        }
    }

    issues
}

/// Run all configured quality checks. Returns issues found.
pub fn run_checks(
    config: &QualityConfig,
    batches: &[RecordBatch],
    total_rows: usize,
) -> Vec<QualityIssue> {
    let mut all = Vec::new();
    all.extend(check_row_count(total_rows, config));
    all.extend(check_null_ratios(batches, &config.null_ratio_max));
    all.extend(check_uniqueness(
        batches,
        &config.unique_columns,
        config.unique_max_entries,
    ));
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    /// The shared failure contract (used by both pipeline gates): names the
    /// export, lists every failing check as a bullet, carries the remediation
    /// hint, and tags the gate when given a context. Tested once here so the
    /// two call sites don't each re-assert it via `contains()`.
    #[test]
    fn failure_message_lists_checks_and_hint() {
        let m = failure_message("orders", None, &["row count 42 < min 100"]);
        assert!(m.contains("export 'orders': 1 quality check(s) failed:"));
        assert!(m.contains("\n  - row count 42 < min 100"));
        assert!(m.contains("adjust the thresholds under `quality:`"));
        assert!(
            !m.contains("aggregate"),
            "no context tag when context is None"
        );

        let c = failure_message("events", Some("chunked aggregate"), &["a", "b"]);
        assert!(c.contains("2 quality check(s) failed (chunked aggregate):"));
        assert!(c.contains("\n  - a\n  - b"));
    }
    use std::sync::Arc;

    fn make_batch(ids: &[Option<i64>], names: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(ids.to_vec());
        let name_arr = StringArray::from(names.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap()
    }

    #[test]
    fn row_count_within_bounds() {
        let cfg = QualityConfig {
            row_count_min: Some(5),
            row_count_max: Some(100),
            null_ratio_max: HashMap::new(),
            unique_columns: vec![],
            unique_max_entries: None,
        };
        assert!(check_row_count(50, &cfg).is_empty());
    }

    #[test]
    fn row_count_below_min() {
        let cfg = QualityConfig {
            row_count_min: Some(100),
            row_count_max: None,
            null_ratio_max: HashMap::new(),
            unique_columns: vec![],
            unique_max_entries: None,
        };
        let issues = check_row_count(50, &cfg);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Fail);
        assert!(issues[0].message.contains("below minimum"));
    }

    #[test]
    fn row_count_above_max() {
        let cfg = QualityConfig {
            row_count_min: None,
            row_count_max: Some(10),
            null_ratio_max: HashMap::new(),
            unique_columns: vec![],
            unique_max_entries: None,
        };
        let issues = check_row_count(50, &cfg);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("exceeds maximum"));
    }

    #[test]
    fn null_ratio_passes() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        assert!(check_null_ratios(&[batch], &thresholds).is_empty());
    }

    #[test]
    fn null_ratio_fails() {
        let batch = make_batch(&[Some(1), Some(2), Some(3)], &[None, None, Some("c")]);
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        let issues = check_null_ratios(&[batch], &thresholds);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("null ratio"));
    }

    #[test]
    fn uniqueness_passes() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let issues = check_uniqueness(&[batch], &["id".into()], None);
        assert!(issues.is_empty());
    }

    #[test]
    fn uniqueness_fails() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(1)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let issues = check_uniqueness(&[batch], &["id".into()], None);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("duplicate"));
    }

    // ─── regression: multi-batch aggregation ─────────────────

    #[test]
    fn null_ratio_multi_batch_aggregates() {
        let b1 = make_batch(&[Some(1), Some(2)], &[None, Some("b")]);
        let b2 = make_batch(&[Some(3), Some(4)], &[None, None]);
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        let issues = check_null_ratios(&[b1, b2], &thresholds);
        assert_eq!(issues.len(), 1, "3/4 nulls > 0.5 threshold");
    }

    #[test]
    fn null_ratio_multi_batch_passes_when_sparse() {
        let b1 = make_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]);
        let b2 = make_batch(&[Some(3)], &[None]);
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        let issues = check_null_ratios(&[b1, b2], &thresholds);
        assert!(issues.is_empty(), "1/3 nulls < 0.5 threshold");
    }

    #[test]
    fn uniqueness_multi_batch_detects_cross_batch_dupes() {
        let b1 = make_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]);
        let b2 = make_batch(&[Some(2), Some(3)], &[Some("c"), Some("d")]);
        let issues = check_uniqueness(&[b1, b2], &["id".into()], None);
        assert_eq!(issues.len(), 1, "id=2 duplicated across batches");
    }

    #[test]
    fn uniqueness_empty_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap();
        let issues = check_uniqueness(&[empty], &["id".into()], None);
        assert!(issues.is_empty(), "empty batch → no duplicates");
    }

    #[test]
    fn null_ratio_empty_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap();
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.0);
        let issues = check_null_ratios(&[empty], &thresholds);
        assert!(issues.is_empty(), "0 rows → skip");
    }

    // ─── regression: run_checks integration ──────────────────

    #[test]
    fn run_checks_combines_all_results() {
        let batch = make_batch(&[Some(1), Some(1), Some(1)], &[None, None, Some("c")]);
        let cfg = QualityConfig {
            row_count_min: Some(100),
            row_count_max: None,
            null_ratio_max: {
                let mut m = HashMap::new();
                m.insert("name".into(), 0.1);
                m
            },
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        };
        let issues = run_checks(&cfg, &[batch], 3);
        assert!(
            issues.len() >= 3,
            "row_count + null_ratio + uniqueness, got: {}",
            issues.len()
        );
    }

    #[test]
    fn run_checks_no_issues_when_clean() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let cfg = QualityConfig {
            row_count_min: Some(1),
            row_count_max: Some(10),
            null_ratio_max: {
                let mut m = HashMap::new();
                m.insert("name".into(), 0.5);
                m
            },
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        };
        let issues = run_checks(&cfg, &[batch], 3);
        assert!(issues.is_empty(), "all clean: {:?}", issues);
    }

    #[test]
    fn row_count_exact_boundary() {
        let cfg = QualityConfig {
            row_count_min: Some(5),
            row_count_max: Some(5),
            null_ratio_max: HashMap::new(),
            unique_columns: vec![],
            unique_max_entries: None,
        };
        assert!(check_row_count(5, &cfg).is_empty(), "exactly on boundary");
        assert!(!check_row_count(4, &cfg).is_empty(), "one below min");
        assert!(!check_row_count(6, &cfg).is_empty(), "one above max");
    }

    #[test]
    fn uniqueness_cap_emits_warn_and_stops() {
        // 5 unique values but cap=3 → should warn; no duplicate issue since we stopped early
        let batch = make_batch(
            &[Some(1), Some(2), Some(3), Some(4), Some(5)],
            &[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
        );
        let issues = check_uniqueness(&[batch], &["id".into()], Some(3));
        assert!(
            issues
                .iter()
                .any(|i| i.severity == Severity::Warn && i.message.contains("capped")),
            "expected cap warning, got: {:?}",
            issues
        );
    }

    #[test]
    fn uniqueness_cap_none_means_no_limit() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let issues = check_uniqueness(&[batch], &["id".into()], None);
        assert!(issues.is_empty(), "no cap → no issues on unique data");
    }

    #[test]
    fn null_ratio_exact_threshold_passes() {
        let batch = make_batch(&[Some(1), Some(2)], &[None, Some("b")]);
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        let issues = check_null_ratios(&[batch], &thresholds);
        assert!(issues.is_empty(), "0.5 == 0.5, not >, so should pass");
    }

    // ─── #33 regression: a quality rule on a ghost column must be LOUD ────────
    // (CLAUDE.md "never a silent no-op"). The old code skipped a missing
    // uniqueness column (index_of → Err → 0 dups → pass) and treated a missing
    // null-ratio column as 0 nulls (unwrap_or(0) → pass); both vanished.

    #[test]
    fn uniqueness_missing_column_is_loud_fail() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let issues = check_uniqueness(&[batch], &["ghost".into()], None);
        assert_eq!(issues.len(), 1, "missing column must produce one Fail");
        assert_eq!(issues[0].severity, Severity::Fail);
        assert!(issues[0].message.contains("ghost"), "names the column");
        assert!(
            issues[0].message.contains("not produced"),
            "explains why: {}",
            issues[0].message
        );
        // Available columns are surfaced so the operator can spot the typo.
        assert!(
            issues[0].message.contains("id") && issues[0].message.contains("name"),
            "lists available columns: {}",
            issues[0].message
        );
    }

    #[test]
    fn uniqueness_missing_column_fails_even_on_empty_batches() {
        // A ghost column is a config error regardless of row count — it must
        // not slip through the `total_rows == 0` early return.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .unwrap();
        let issues = check_uniqueness(&[empty], &["ghost".into()], None);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Fail);
        assert!(issues[0].message.contains("ghost"));
    }

    #[test]
    fn null_ratio_missing_column_is_loud_fail() {
        let batch = make_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]);
        let mut thresholds = HashMap::new();
        thresholds.insert("ghost".into(), 0.5);
        let issues = check_null_ratios(&[batch], &thresholds);
        assert_eq!(issues.len(), 1, "missing column must produce one Fail");
        assert_eq!(issues[0].severity, Severity::Fail);
        assert!(issues[0].message.contains("ghost"));
        assert!(issues[0].message.contains("not produced"));
    }

    #[test]
    fn validate_quality_columns_ok_when_all_present() {
        let cfg = QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: {
                let mut m = HashMap::new();
                m.insert("name".into(), 0.5);
                m
            },
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        };
        let available = vec!["id".to_string(), "name".to_string()];
        assert!(validate_quality_columns(&cfg, &available).is_ok());
    }

    #[test]
    fn validate_quality_columns_errors_naming_column_and_available() {
        let cfg = QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: HashMap::new(),
            unique_columns: vec!["ghost".into()],
            unique_max_entries: None,
        };
        let available = vec!["id".to_string(), "name".to_string()];
        let err =
            validate_quality_columns(&cfg, &available).expect_err("ghost column must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("ghost"), "names the offending column: {msg}");
        assert!(
            msg.contains("id") && msg.contains("name"),
            "lists available columns: {msg}"
        );
    }
}
