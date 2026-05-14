//! Data shape drift tracking (Epic 8).
//!
//! Tracks the maximum observed byte length per string/binary column across runs.
//! On each run the current-run maxima are compared against the stored maxima;
//! columns that grew beyond `warn_factor × stored_max` are returned as warnings.
//! Stored maxima are always updated to `max(stored, current)` — shape drift
//! tracking is advisory and never blocks a run.

use std::collections::HashMap;

use crate::error::Result;

use super::{StateConn, StateStore, pg_sql};

/// One column whose observed max byte length grew beyond the configured threshold.
pub struct ShapeWarning {
    pub column: String,
    pub stored_max_bytes: u64,
    pub current_max_bytes: u64,
    /// `current_max_bytes / stored_max_bytes` — always > `warn_factor`.
    pub growth_factor: f64,
}

impl StateStore {
    /// Return the stored per-column max byte lengths for `export_name`.
    pub fn get_shape_stats(&self, export_name: &str) -> Result<HashMap<String, u64>> {
        let sql = "SELECT column_name, max_byte_len FROM export_shape WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map([export_name], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })?;
                let mut map = HashMap::new();
                for r in rows {
                    let (k, v) = r?;
                    map.insert(k, v);
                }
                Ok(map)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[&export_name])?;
                let mut map = HashMap::new();
                for row in rows {
                    let k: String = row.get(0);
                    let v: i64 = row.get(1);
                    map.insert(k, v as u64);
                }
                Ok(map)
            }
        }
    }

    /// Upsert per-column max byte lengths, keeping the running maximum.
    pub fn store_shape_stats(&self, export_name: &str, stats: &HashMap<String, u64>) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_shape (export_name, column_name, max_byte_len, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(export_name, column_name) DO UPDATE SET
                     max_byte_len = MAX(max_byte_len, excluded.max_byte_len),
                     updated_at   = excluded.updated_at";
        match &self.conn {
            StateConn::Sqlite(c) => {
                for (col, &max_bytes) in stats {
                    c.execute(
                        sql,
                        rusqlite::params![export_name, col, max_bytes as i64, now],
                    )?;
                }
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                for (col, &max_bytes) in stats {
                    c.execute(
                        &pg_sql(sql),
                        &[&export_name, col, &(max_bytes as i64), &now],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Compare `current` run's per-column maxima against stored history.
    ///
    /// Returns a warning for every column whose `current_max > stored_max * warn_factor`.
    /// The stored maxima are updated to `max(stored, current)` unconditionally so that
    /// the running high-water mark is always current.
    ///
    /// First-run columns (no stored record) are silently accepted.
    pub fn detect_shape_drift(
        &self,
        export_name: &str,
        current: &HashMap<String, u64>,
        warn_factor: f64,
    ) -> Result<Vec<ShapeWarning>> {
        let stored = self.get_shape_stats(export_name)?;
        let mut warnings = Vec::new();

        for (col, &current_max) in current {
            if let Some(&stored_max) = stored.get(col)
                && stored_max > 0
                && (current_max as f64) > stored_max as f64 * warn_factor
            {
                warnings.push(ShapeWarning {
                    column: col.clone(),
                    stored_max_bytes: stored_max,
                    current_max_bytes: current_max,
                    growth_factor: current_max as f64 / stored_max as f64,
                });
            }
        }

        self.store_shape_stats(export_name, current)?;
        Ok(warnings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn first_run_no_warnings() {
        let s = store();
        let stats: HashMap<String, u64> =
            [("notes".into(), 512u64), ("description".into(), 1024u64)].into();
        let warnings = s.detect_shape_drift("orders", &stats, 2.0).unwrap();
        assert!(warnings.is_empty(), "first run must not warn");
    }

    #[test]
    fn growth_below_threshold_no_warning() {
        let s = store();
        let v1: HashMap<String, u64> = [("body".into(), 1000u64)].into();
        s.detect_shape_drift("t", &v1, 2.0).unwrap();

        let v2: HashMap<String, u64> = [("body".into(), 1800u64)].into();
        let warnings = s.detect_shape_drift("t", &v2, 2.0).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn growth_above_threshold_warns() {
        let s = store();
        let v1: HashMap<String, u64> = [("body".into(), 1000u64)].into();
        s.detect_shape_drift("t", &v1, 2.0).unwrap();

        let v2: HashMap<String, u64> = [("body".into(), 2500u64)].into();
        let warnings = s.detect_shape_drift("t", &v2, 2.0).unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].column, "body");
        assert_eq!(warnings[0].stored_max_bytes, 1000);
        assert_eq!(warnings[0].current_max_bytes, 2500);
        assert!((warnings[0].growth_factor - 2.5).abs() < 0.01);
    }

    #[test]
    fn high_water_mark_advances_after_warning() {
        let s = store();
        let v1: HashMap<String, u64> = [("text".into(), 100u64)].into();
        s.detect_shape_drift("t", &v1, 2.0).unwrap();

        let v2: HashMap<String, u64> = [("text".into(), 300u64)].into();
        s.detect_shape_drift("t", &v2, 2.0).unwrap();

        let v3: HashMap<String, u64> = [("text".into(), 450u64)].into();
        let warnings = s.detect_shape_drift("t", &v3, 2.0).unwrap();
        assert!(
            warnings.is_empty(),
            "must not re-warn after high-water mark advanced"
        );
    }

    #[test]
    fn new_column_in_later_run_no_warning() {
        let s = store();
        let v1: HashMap<String, u64> = [("id_str".into(), 36u64)].into();
        s.detect_shape_drift("t", &v1, 2.0).unwrap();

        let v2: HashMap<String, u64> =
            [("id_str".into(), 36u64), ("new_col".into(), 9999u64)].into();
        let warnings = s.detect_shape_drift("t", &v2, 2.0).unwrap();
        assert!(
            warnings.is_empty(),
            "new columns with no history must not warn"
        );
    }
}
