use std::collections::{HashMap, HashSet};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use crate::config::QualityConfig;

#[derive(Debug, Clone)]
pub struct QualityIssue {
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
    #[allow(dead_code)]
    Warn,
    Fail,
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

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Vec::new();
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

    let mut issues = Vec::new();
    for (col_name, max_ratio) in thresholds {
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

#[allow(dead_code)]
pub fn check_uniqueness(batches: &[RecordBatch], columns: &[String]) -> Vec<QualityIssue> {
    if columns.is_empty() {
        return Vec::new();
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Vec::new();
    }

    let mut issues = Vec::new();

    for col_name in columns {
        let mut seen = HashSet::new();
        let mut duplicates = 0usize;

        for batch in batches {
            if let Ok(idx) = batch.schema().index_of(col_name) {
                let col = batch.column(idx);
                let string_col = arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &arrow::util::display::FormatOptions::default(),
                );
                if let Ok(formatter) = string_col {
                    for row in 0..col.len() {
                        let val = formatter.value(row).to_string();
                        if !seen.insert(val) {
                            duplicates += 1;
                        }
                    }
                }
            }
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
#[allow(dead_code)]
pub fn run_checks(
    config: &QualityConfig,
    batches: &[RecordBatch],
    total_rows: usize,
) -> Vec<QualityIssue> {
    let mut all = Vec::new();
    all.extend(check_row_count(total_rows, config));
    all.extend(check_null_ratios(batches, &config.null_ratio_max));
    all.extend(check_uniqueness(batches, &config.unique_columns));
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
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
        let issues = check_uniqueness(&[batch], &["id".into()]);
        assert!(issues.is_empty());
    }

    #[test]
    fn uniqueness_fails() {
        let batch = make_batch(
            &[Some(1), Some(2), Some(1)],
            &[Some("a"), Some("b"), Some("c")],
        );
        let issues = check_uniqueness(&[batch], &["id".into()]);
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
        let issues = check_uniqueness(&[b1, b2], &["id".into()]);
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
        let issues = check_uniqueness(&[empty], &["id".into()]);
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
        };
        assert!(check_row_count(5, &cfg).is_empty(), "exactly on boundary");
        assert!(!check_row_count(4, &cfg).is_empty(), "one below min");
        assert!(!check_row_count(6, &cfg).is_empty(), "one above max");
    }

    #[test]
    fn null_ratio_exact_threshold_passes() {
        let batch = make_batch(&[Some(1), Some(2)], &[None, Some("b")]);
        let mut thresholds = HashMap::new();
        thresholds.insert("name".into(), 0.5);
        let issues = check_null_ratios(&[batch], &thresholds);
        assert!(issues.is_empty(), "0.5 == 0.5, not >, so should pass");
    }
}
