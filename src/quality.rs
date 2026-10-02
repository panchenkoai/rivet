//! The export's quality gate: the declared rules, the streaming tracker the sink
//! feeds every batch, and the operator-facing failure contract.

use std::collections::{HashMap, HashSet};

use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use xxhash_rust::xxh3::xxh3_64;

use crate::config::QualityConfig;
use crate::enrich::{CanonColumn, CanonUse};
use crate::error::Result;

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

/// The operator-facing quality-gate failure contract: the "N check(s) failed" body, one
/// bullet per failing check, and the remediation hint.
pub fn failure_message(export_name: &str, failing: &[&str]) -> String {
    format!(
        "export '{}': {} quality check(s) failed:\n  - {}\n  \
         Fix the source data, or adjust the thresholds under `quality:` in your config.",
        export_name,
        failing.len(),
        failing.join("\n  - "),
    )
}

/// Pre-flight gate (#33, column-applicability): every column named by a quality
/// rule must be produced by the export, or the gate is a silent no-op — the
/// uniqueness loop `index_of(col)` skips a missing column (0 duplicates, "pass")
/// and the null-ratio loop's `unwrap_or(0)` treats a missing column as 0 nulls
/// ("pass"). Per the process rules ("never a silent no-op"), a quality rule that can
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

/// The export's declared quality rules and what has been measured against them.
///
/// Seven of `ExportSink`'s fields were this one concern, and nothing in the write path
/// reads them: the tracker needs the batch, the resolved dest schema, and the run's row
/// count, and it answers with issues. Every sink of a run feeds one; the runner hands it
/// on as an observation (`pipeline::commit::Observations`), the ledger merges them, and
/// the finalize seam grades the merged tracker once for every runner.
#[derive(Debug, Clone, Default)]
pub(crate) struct QualityTracker {
    pub(crate) columns: Option<QualityConfig>,
    pub(crate) null_counts: HashMap<String, usize>,
    pub(crate) unique_sets: HashMap<String, HashSet<u64>>,
    /// Per-column count of non-NULL values seen by uniqueness tracking. NULLs are never
    /// duplicates (SQL UNIQUE semantics) and are skipped from hashing, so duplicates must
    /// be computed against this count, not the run's `total_rows`.
    pub(crate) unique_non_null_counts: HashMap<String, usize>,
    /// Columns whose unique-entry tracking stopped because `unique_max_entries` was reached.
    pub(crate) unique_capped: HashSet<String>,
    /// Column index caches, built once when the dest schema resolves.
    pub(crate) null_indices: Vec<(usize, String)>,
    pub(crate) unique_indices: Vec<(usize, String)>,
}

impl QualityTracker {
    pub(crate) fn new(columns: Option<QualityConfig>) -> Self {
        Self {
            columns,
            ..Default::default()
        }
    }

    /// Bind the declared rules to the resolved dest schema, caching each rule's column
    /// index.
    ///
    /// Fails loud (#33, "never a silent no-op"): a rule naming a column the export does not
    /// produce would otherwise be dropped by the filters below and report `quality: pass`
    /// over a gate that never ran. Validated the moment the schema resolves, before any
    /// batch.
    pub(crate) fn resolve_columns(&mut self, dest_schema: &Schema) -> Result<()> {
        let Some(qc) = &self.columns else {
            return Ok(());
        };
        // A zero-row result arrives as `Schema::empty()`: no columns to bind, no cells to measure.
        if dest_schema.fields().is_empty() {
            return Ok(());
        }
        let available: Vec<String> = dest_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        validate_quality_columns(qc, &available)?;
        self.null_indices = dest_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| qc.null_ratio_max.contains_key(f.name().as_str()))
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        self.unique_indices = dest_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| qc.unique_columns.contains(f.name()))
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        Ok(())
    }

    /// Accumulate one batch against the declared rules; a present cell that cannot be canonicalized fails the run.
    pub(crate) fn track(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.columns.is_none() {
            return Ok(());
        }
        for (i, name) in &self.null_indices {
            *self.null_counts.entry(name.clone()).or_default() += batch.column(*i).null_count();
        }
        let cap = self.columns.as_ref().and_then(|q| q.unique_max_entries);
        let mut scratch = Vec::with_capacity(64);
        for (i, name) in &self.unique_indices {
            if self.unique_capped.contains(name) {
                continue;
            }
            let col = batch.column(*i);
            let canon = CanonColumn::new(col.as_ref(), name, CanonUse::Unique)?;
            let non_null_count = self.unique_non_null_counts.entry(name.clone()).or_default();
            let set = self.unique_sets.entry(name.clone()).or_default();
            for row in 0..col.len() {
                // NULLs are never duplicates (SQL UNIQUE semantics): skip before the
                // cap check so trailing NULLs can't trip the cap.
                if col.is_null(row) {
                    continue;
                }
                if let Some(limit) = cap
                    && set.len() >= limit
                {
                    self.unique_capped.insert(name.clone());
                    break;
                }
                scratch.clear();
                canon.write(&mut scratch, row)?;
                set.insert(xxh3_64(&scratch));
                *non_null_count += 1;
            }
        }
        Ok(())
    }

    /// Fold another sink's measurements in: null counts add, distinct sets union up to
    /// `unique_max_entries` (counting only the values folded in), caps union.
    pub(crate) fn merge(&mut self, other: QualityTracker) {
        let QualityTracker {
            columns,
            null_counts,
            unique_sets,
            unique_non_null_counts,
            unique_capped,
            ..
        } = other;
        if self.columns.is_none() {
            self.columns = columns;
        }
        let cap = self.columns.as_ref().and_then(|q| q.unique_max_entries);
        for (col, n) in null_counts {
            *self.null_counts.entry(col).or_default() += n;
        }
        self.unique_capped.extend(unique_capped);
        for (col, set) in unique_sets {
            let own_dupes = unique_non_null_counts
                .get(&col)
                .copied()
                .unwrap_or(0)
                .saturating_sub(set.len());
            let mine = self.unique_sets.entry(col.clone()).or_default();
            let mut folded = 0;
            for h in set {
                if cap.is_some_and(|c| mine.len() >= c) {
                    self.unique_capped.insert(col.clone());
                    break;
                }
                mine.insert(h);
                folded += 1;
            }
            *self.unique_non_null_counts.entry(col).or_default() += own_dupes + folded;
        }
    }

    /// The verdict against the tracker's own rules, given the run's row count.
    #[cfg(test)]
    pub(crate) fn issues(&self, total_rows: usize) -> Vec<QualityIssue> {
        match &self.columns {
            Some(qc) => self.issues_for(qc, total_rows),
            None => Vec::new(),
        }
    }

    /// The verdict against `qc`, given the run's row count — the one number the rules
    /// need that the tracker does not own.
    pub(crate) fn issues_for(&self, qc: &QualityConfig, total_rows: usize) -> Vec<QualityIssue> {
        let mut issues = Vec::new();
        issues.extend(check_row_count(total_rows, qc));
        if total_rows == 0 {
            return issues;
        }
        for (col, max_ratio) in &qc.null_ratio_max {
            let nulls = self.null_counts.get(col).copied().unwrap_or(0);
            let ratio = nulls as f64 / total_rows as f64;
            if ratio > *max_ratio {
                issues.push(QualityIssue {
                    severity: Severity::Fail,
                    message: format!(
                        "column '{}': null ratio {:.4} exceeds threshold {:.4}",
                        col, ratio, max_ratio
                    ),
                });
            }
        }
        for col in &qc.unique_columns {
            let capped = self.unique_capped.contains(col);
            if capped {
                let cap = qc.unique_max_entries.unwrap_or(0);
                issues.push(QualityIssue {
                    severity: Severity::Warn,
                    message: format!(
                        "column '{}': uniqueness check capped at {} entries; result may be \
                         incomplete (set unique_max_entries higher to cover all rows)",
                        col, cap
                    ),
                });
            }
            if let Some(set) = self.unique_sets.get(col) {
                let non_null = self.unique_non_null_counts.get(col).copied().unwrap_or(0);
                let dupes = non_null.saturating_sub(set.len());
                if dupes > 0 {
                    let at_least = if capped { "at least " } else { "" };
                    issues.push(QualityIssue {
                        severity: Severity::Fail,
                        message: format!(
                            "column '{}': {}{} duplicate values out of {} rows",
                            col, at_least, dupes, total_rows
                        ),
                    });
                }
            }
        }
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    /// The failure contract names the export, bullets every failing check, and carries
    /// the remediation hint.
    #[test]
    fn failure_message_lists_checks_and_hint() {
        let m = failure_message("orders", &["row count 42 < min 100"]);
        assert!(m.contains("export 'orders': 1 quality check(s) failed:"));
        assert!(m.contains("\n  - row count 42 < min 100"));
        assert!(m.contains("adjust the thresholds under `quality:`"));

        let c = failure_message("events", &["a", "b"]);
        assert!(c.contains("2 quality check(s) failed:\n  - a\n  - b"));
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

    fn unique_tracker(col: &str, schema: &Schema) -> QualityTracker {
        let mut t = QualityTracker::new(Some(QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: HashMap::new(),
            unique_columns: vec![col.into()],
            unique_max_entries: None,
        }));
        t.resolve_columns(schema).unwrap();
        t
    }

    fn list_batch(rows: Vec<Option<Vec<Option<&str>>>>) -> RecordBatch {
        use arrow::array::{ListBuilder, StringBuilder};
        let mut b = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                Some(items) => {
                    for item in items {
                        b.values().append_option(item);
                    }
                    b.append(true);
                }
                None => b.append(false),
            }
        }
        let arr = b.finish();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            arr.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn fails(issues: &[QualityIssue]) -> Vec<&str> {
        issues
            .iter()
            .filter(|i| i.severity == Severity::Fail)
            .map(|i| i.message.as_str())
            .collect()
    }

    #[test]
    fn unique_list_cells_whose_display_text_agrees_are_not_duplicates() {
        let batch = list_batch(vec![
            Some(vec![Some("a, b")]),
            Some(vec![Some("a"), Some("b")]),
        ]);
        let mut t = unique_tracker("tags", &batch.schema());
        t.track(&batch).unwrap();
        assert_eq!(fails(&t.issues(2)), Vec::<&str>::new());
    }

    #[test]
    fn unique_null_element_empty_string_and_empty_list_are_three_values() {
        let batch = list_batch(vec![Some(vec![None]), Some(vec![Some("")]), Some(vec![])]);
        let mut t = unique_tracker("tags", &batch.schema());
        t.track(&batch).unwrap();
        assert_eq!(fails(&t.issues(3)), Vec::<&str>::new());
    }

    #[test]
    fn unique_identical_lists_are_one_duplicate() {
        let batch = list_batch(vec![
            Some(vec![Some("x"), Some("y")]),
            Some(vec![Some("x"), Some("y")]),
            None,
        ]);
        let mut t = unique_tracker("tags", &batch.schema());
        t.track(&batch).unwrap();
        assert_eq!(
            fails(&t.issues(3)),
            vec!["column 'tags': 1 duplicate values out of 3 rows"]
        );
    }

    #[test]
    fn unique_detects_a_duplicate_across_batches() {
        let b1 = make_batch(&[Some(1), Some(2)], &[Some("a"), Some("b")]);
        let b2 = make_batch(&[Some(2), None], &[Some("c"), Some("d")]);
        let mut t = unique_tracker("id", &b1.schema());
        t.track(&b1).unwrap();
        t.track(&b2).unwrap();
        assert_eq!(
            fails(&t.issues(4)),
            vec!["column 'id': 1 duplicate values out of 4 rows"]
        );
    }

    /// Pins the refusal policy: an unparseable zone is the one type Arrow's formatter
    /// refuses that a batch can carry, since `chrono-tz` renders every zone rivet emits.
    #[test]
    fn unique_on_a_column_that_cannot_be_rendered_refuses_instead_of_passing() {
        use arrow::array::{ArrayRef, TimestampMicrosecondArray};
        let arr: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_700_000_000_000_000i64; 2])
                .with_timezone("Mars/Olympus".to_string()),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "seen_at",
            arr.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let mut t = unique_tracker("seen_at", &schema);
        let err = t
            .track(&batch)
            .expect_err("an unrenderable column must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.starts_with("quality.unique_columns: column 'seen_at'")
                && msg.ends_with("Remove it from `quality.unique_columns`."),
            "{msg}"
        );
    }

    /// Two sinks that each saw one copy of a value hold no duplicate alone; merged they
    /// hold one, their null counts add into one ratio, and a cap on either side caps the whole.
    #[test]
    fn merge_finds_a_duplicate_split_across_two_sinks_and_adds_nulls() {
        let b1 = make_batch(&[Some(1), Some(2)], &[Some("a"), None]);
        let b2 = make_batch(&[Some(2), Some(3)], &[None, None]);
        let qc = QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: [("name".to_string(), 0.5)].into(),
            unique_columns: vec!["id".into()],
            unique_max_entries: Some(10),
        };
        let mut t1 = QualityTracker::new(Some(qc.clone()));
        t1.resolve_columns(&b1.schema()).unwrap();
        t1.track(&b1).unwrap();
        let mut t2 = QualityTracker::new(Some(qc.clone()));
        t2.resolve_columns(&b2.schema()).unwrap();
        t2.track(&b2).unwrap();
        let dupes =
            |issues: Vec<QualityIssue>| issues.iter().any(|i| i.message.contains("duplicate"));
        assert!(
            !dupes(t1.issues_for(&qc, 2)) && !dupes(t2.issues(2)),
            "neither sink alone holds the duplicate"
        );

        let mut merged = QualityTracker::default();
        merged.merge(t1);
        merged.merge(t2);
        assert_eq!(merged.null_counts.get("name"), Some(&3));
        assert_eq!(merged.unique_non_null_counts.get("id"), Some(&4));
        assert_eq!(
            fails(&merged.issues_for(&qc, 4)),
            vec![
                "column 'name': null ratio 0.7500 exceeds threshold 0.5000",
                "column 'id': 1 duplicate values out of 4 rows",
            ]
        );
        assert!(merged.columns.is_some(), "the first fed rules are kept");
        assert!(
            QualityTracker::default().issues(4).is_empty(),
            "a tracker with no rules has no verdict of its own"
        );

        let mut capped = QualityTracker::default();
        capped.unique_capped.insert("id".into());
        merged.merge(capped);
        assert!(merged.unique_capped.contains("id"));
    }

    /// An empty page / chunk / range arrives as `Schema::empty()`; it measures nothing and
    /// must not be read as "the rule's column is missing" (it killed every keyset run
    /// whose range ended on an empty page). A real schema without the column still refuses.
    #[test]
    fn a_zero_row_empty_schema_binds_nothing_and_is_not_a_ghost_column() {
        let mut t = unique_tracker("id", &make_batch(&[], &[]).schema());
        t.resolve_columns(&Schema::empty())
            .expect("an empty schema is a zero-row result, not a missing column");
        let other = Schema::new(vec![Field::new("other", DataType::Int64, true)]);
        assert!(
            t.resolve_columns(&other).is_err(),
            "a real schema still refuses"
        );
    }

    /// The merged distinct set stays inside `unique_max_entries`, and a value left out at
    /// the cap is not counted, so the capped column warns instead of reporting a phantom
    /// duplicate; one sink's own duplicates survive the merge.
    #[test]
    fn merge_stops_at_the_unique_cap_without_inventing_duplicates() {
        let qc = QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: HashMap::new(),
            unique_columns: vec!["id".into()],
            unique_max_entries: Some(4),
        };
        let sink = |ids: &[u64], non_null: usize| {
            let mut t = QualityTracker::new(Some(qc.clone()));
            t.unique_sets
                .insert("id".into(), ids.iter().copied().collect());
            t.unique_non_null_counts.insert("id".into(), non_null);
            t
        };
        let mut merged = sink(&[1, 2, 3], 4); // one duplicate inside this sink
        merged.merge(sink(&[4, 5], 2));
        assert_eq!(
            merged.unique_sets["id"].len(),
            4,
            "the union stops at the cap"
        );
        assert!(merged.unique_capped.contains("id"));
        assert_eq!(merged.unique_non_null_counts["id"], 5, "4 seen + 1 folded");
        assert_eq!(
            fails(&merged.issues(9)),
            vec!["column 'id': at least 1 duplicate values out of 9 rows"]
        );
    }

    // ─── #33 regression: a quality rule on a ghost column must be LOUD ────────

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
