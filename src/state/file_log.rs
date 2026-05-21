use crate::error::Result;

use super::{StateConn, StateStore, pg_sql};

/// One row from `file_log` (formerly `file_manifest`; renamed in schema v8).
#[derive(Debug)]
#[allow(dead_code)]
pub struct FileRecord {
    pub run_id: String,
    pub export_name: String,
    pub file_name: String,
    pub row_count: i64,
    pub bytes: i64,
    pub format: String,
    pub compression: Option<String>,
    pub created_at: String,
}

/// File log store — reads and writes `file_log`.
///
/// Historical note: this table was named `file_manifest` prior to schema v8.
/// The name was reclaimed for the 0.7.0 cloud-output JSON manifest contract;
/// the internal SQLite log was renamed to `file_log` to remove the overload.
///
/// Invariant I2 (Write Before Log) governs when `record_file` is called:
/// only after a destination write succeeds.  Failed writes produce no log entry.
/// Invariant I7 (File-Log Failure Is Non-Fatal) means callers use `let _ = record_file(...)`.
impl StateStore {
    #[allow(clippy::too_many_arguments)]
    pub fn record_file(
        &self,
        run_id: &str,
        export_name: &str,
        file_name: &str,
        row_count: i64,
        bytes: i64,
        format: &str,
        compression: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, compression, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![
                        run_id,
                        export_name,
                        file_name,
                        row_count,
                        bytes,
                        format,
                        compression,
                        now
                    ],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[
                        &run_id,
                        &export_name,
                        &file_name,
                        &row_count,
                        &bytes,
                        &format,
                        &compression,
                        &now,
                    ],
                )?;
            }
        }
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        let limit_i64 = limit as i64;
        match &self.conn {
            StateConn::Sqlite(c) => {
                let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(
                    name,
                ) = export_name
                {
                    (
                        "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at \
                             FROM file_log WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2",
                        vec![Box::new(name.to_string()), Box::new(limit_i64)],
                    )
                } else {
                    (
                        "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at \
                             FROM file_log ORDER BY id DESC LIMIT ?1",
                        vec![Box::new(limit_i64)],
                    )
                };
                let mut stmt = c.prepare(sql)?;
                let params_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                let rows = stmt.query_map(params_refs.as_slice(), |row| {
                    Ok(FileRecord {
                        run_id: row.get(0)?,
                        export_name: row.get(1)?,
                        file_name: row.get(2)?,
                        row_count: row.get(3)?,
                        bytes: row.get(4)?,
                        format: row.get(5)?,
                        compression: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            StateConn::Postgres(client) => {
                // Single borrow for the duration of this call; safe because all Postgres
                // operations in StateStore are sequential (no re-entrant borrows).
                let mut c = client.borrow_mut();
                let rows = if let Some(name) = export_name {
                    c.query(
                        &format!("SELECT {} FROM file_log WHERE export_name = $1 ORDER BY id DESC LIMIT $2", cols),
                        &[&name, &limit_i64],
                    )?
                } else {
                    c.query(
                        &format!("SELECT {} FROM file_log ORDER BY id DESC LIMIT $1", cols),
                        &[&limit_i64],
                    )?
                };
                Ok(rows
                    .iter()
                    .map(|row| FileRecord {
                        run_id: row.get(0),
                        export_name: row.get(1),
                        file_name: row.get(2),
                        row_count: row.get(3),
                        bytes: row.get(4),
                        format: row.get(5),
                        compression: row.get(6),
                        created_at: row.get(7),
                    })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn record_and_query_files() {
        let s = store();
        s.record_file(
            "run_001",
            "orders",
            "orders_20260329.parquet",
            50000,
            4096,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
        s.record_file(
            "run_001",
            "orders",
            "orders_20260329_chunk1.parquet",
            25000,
            2048,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
        s.record_file(
            "run_002",
            "users",
            "users_20260329.csv",
            1000,
            500,
            "csv",
            None,
        )
        .unwrap();

        let files = s.get_files(Some("orders"), 10).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].run_id, "run_001");
        assert_eq!(files[0].row_count, 25000);

        let all = s.get_files(None, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn files_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_file(
                &format!("r{}", i),
                "t",
                &format!("f{}.parquet", i),
                i,
                i * 100,
                "parquet",
                None,
            )
            .unwrap();
        }
        let files = s.get_files(Some("t"), 3).unwrap();
        assert_eq!(files.len(), 3);
    }
}
