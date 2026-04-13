use crate::error::Result;

use super::StateStore;

/// One row from `file_manifest`.
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

/// File manifest store — reads and writes `file_manifest`.
///
/// Invariant I2 (Write Before Manifest) governs when `record_file` is called:
/// only after a destination write succeeds.  Failed writes produce no manifest entry.
/// Invariant I7 (Manifest Failure Is Non-Fatal) means callers use `let _ = record_file(...)`.
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
        self.conn.execute(
            "INSERT INTO file_manifest (run_id, export_name, file_name, row_count, bytes, format, compression, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![run_id, export_name, file_name, row_count, bytes, format, compression, now],
        )?;
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(name) =
            export_name
        {
            (
                    format!(
                        "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at
                         FROM file_manifest WHERE export_name = ?1 ORDER BY id DESC LIMIT {}",
                        limit
                    ),
                    vec![Box::new(name.to_string())],
                )
        } else {
            (
                    format!(
                        "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at
                         FROM file_manifest ORDER BY id DESC LIMIT {}",
                        limit
                    ),
                    vec![],
                )
        };

        let mut stmt = self.conn.prepare(&sql)?;
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
