use crate::error::Result;

use super::StateStore;

/// One row from `file_log` (formerly `file_manifest`; renamed in schema v8).
#[derive(Debug, serde::Serialize)]
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
        self.execute(
            "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, compression, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                run_id.into(),
                export_name.into(),
                file_name.into(),
                row_count.into(),
                bytes.into(),
                format.into(),
                compression.into(),
                chrono::Utc::now().to_rfc3339().into(),
            ],
        )?;
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        match export_name {
            Some(name) => self.query(
                &format!(
                    "SELECT {cols} FROM file_log WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2"
                ),
                &[name.into(), (limit as i64).into()],
                file_record,
            ),
            None => self.query(
                &format!("SELECT {cols} FROM file_log ORDER BY id DESC LIMIT ?1"),
                &[(limit as i64).into()],
                file_record,
            ),
        }
    }

    /// Every `file_log` row for one `run_id`, in write order. Unlike `chunk_task`
    /// (one `file_name` per chunk, the FIRST rotation sibling only), file_log
    /// records EVERY committed part — including all `max_file_size` rotation
    /// siblings — with its real `bytes`, written per-part before `complete_chunk_
    /// task`. This is the crash-consistent source the chunked resume reconstructs a
    /// COMPLETE destination manifest from when none exists (round-5 fix).
    pub fn list_files_for_run(&self, run_id: &str) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        self.query(
            &format!("SELECT {cols} FROM file_log WHERE run_id = ?1 ORDER BY id ASC"),
            &[run_id.into()],
            file_record,
        )
    }
}

/// The `FileRecord` projection, written once for both backends.
fn file_record(r: &dyn super::row::StateRow) -> FileRecord {
    FileRecord {
        run_id: r.text(0),
        export_name: r.text(1),
        file_name: r.text(2),
        row_count: r.i64(3),
        bytes: r.i64(4),
        format: r.text(5),
        compression: r.opt_text(6),
        created_at: r.text(7),
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
