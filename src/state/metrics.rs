use crate::error::Result;

use super::StateStore;

/// One row from `export_metrics`.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ExportMetric {
    pub export_name: String,
    pub run_id: Option<String>,
    pub run_at: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
}

/// Metrics store — reads and writes `export_metrics`.
///
/// Invariant I4 (Metric After Verdict) governs when `record_metric` is called:
/// only after the terminal run outcome is determined.
impl StateStore {
    #[allow(clippy::too_many_arguments)]
    pub fn record_metric(
        &self,
        export_name: &str,
        run_id: &str,
        duration_ms: i64,
        total_rows: i64,
        peak_rss_mb: Option<i64>,
        status: &str,
        error_message: Option<&str>,
        tuning_profile: Option<&str>,
        format: Option<&str>,
        mode: Option<&str>,
        files_produced: i64,
        bytes_written: i64,
        retries: i64,
        validated: Option<bool>,
        schema_changed: Option<bool>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_metrics (export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
             status, error_message, tuning_profile, format, mode,
             files_produced, bytes_written, retries, validated, schema_changed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                export_name, run_id, now, duration_ms, total_rows, peak_rss_mb,
                status, error_message, tuning_profile, format, mode,
                files_produced, bytes_written, retries, validated, schema_changed
            ],
        )?;
        Ok(())
    }

    pub fn get_metrics(
        &self,
        export_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ExportMetric>> {
        let cols = "export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
                    status, error_message, tuning_profile, format, mode,
                    files_produced, bytes_written, retries, validated, schema_changed";
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(name) =
            export_name
        {
            (
                format!(
                    "SELECT {} FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT {}",
                    cols, limit
                ),
                vec![Box::new(name.to_string())],
            )
        } else {
            (
                format!(
                    "SELECT {} FROM export_metrics ORDER BY id DESC LIMIT {}",
                    cols, limit
                ),
                vec![],
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(ExportMetric {
                export_name: row.get(0)?,
                run_id: row.get(1)?,
                run_at: row.get(2)?,
                duration_ms: row.get(3)?,
                total_rows: row.get(4)?,
                peak_rss_mb: row.get(5)?,
                status: row.get(6)?,
                error_message: row.get(7)?,
                tuning_profile: row.get(8)?,
                format: row.get(9)?,
                mode: row.get(10)?,
                files_produced: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
                bytes_written: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                retries: row.get::<_, Option<i64>>(13)?.unwrap_or(0),
                validated: row.get(14)?,
                schema_changed: row.get(15)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn record_and_query_metrics() {
        let s = store();
        s.record_metric(
            "orders",
            "run_001",
            1200,
            50000,
            Some(142),
            "success",
            None,
            Some("safe"),
            Some("parquet"),
            Some("full"),
            1,
            4096,
            0,
            Some(true),
            Some(false),
        )
        .unwrap();
        s.record_metric(
            "orders",
            "run_002",
            300,
            0,
            Some(30),
            "failed",
            Some("timeout"),
            Some("safe"),
            Some("parquet"),
            Some("full"),
            0,
            0,
            2,
            None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(Some("orders"), 10).unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].status, "failed");
        assert_eq!(metrics[0].run_id.as_deref(), Some("run_002"));
        assert_eq!(metrics[0].retries, 2);
        assert_eq!(metrics[1].total_rows, 50000);
        assert_eq!(metrics[1].run_id.as_deref(), Some("run_001"));
        assert_eq!(metrics[1].files_produced, 1);
        assert_eq!(metrics[1].bytes_written, 4096);
        assert_eq!(metrics[1].validated, Some(true));
        assert_eq!(metrics[1].schema_changed, Some(false));
    }

    #[test]
    fn query_metrics_all_exports() {
        let s = store();
        s.record_metric(
            "orders", "r1", 100, 1000, None, "success", None, None, None, None, 1, 500, 0, None,
            None,
        )
        .unwrap();
        s.record_metric(
            "users", "r2", 200, 2000, None, "success", None, None, None, None, 1, 800, 0, None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(None, 10).unwrap();
        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn metrics_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_metric(
                "t",
                &format!("r{}", i),
                i * 100,
                i,
                None,
                "success",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
                None,
            )
            .unwrap();
        }
        let metrics = s.get_metrics(Some("t"), 3).unwrap();
        assert_eq!(metrics.len(), 3);
    }
}
