//! ORACLE — reading back what a run delivered. The invariant this module
//! owns: the read-back must FOLLOW the destination (a cloud/stdout rig is
//! refused rather than read as a vacuously-empty local dir — bughunt find),
//! and a missing dir is a harness bug, never "zero parts".

use super::*;

impl Rig {
    pub fn out_dir(&self) -> PathBuf {
        self.dest_override
            .clone()
            .unwrap_or_else(|| self.dir.path().join("out"))
    }

    /// Destination directory of a named export — the primary or any secondary.
    pub fn out_dir_for(&self, name: &str) -> PathBuf {
        if name == self.name {
            return self.out_dir();
        }
        self.dir.path().join(format!("out_{name}"))
    }

    /// Run and read every parquet part back — the canonical
    /// capture-and-verify shape the outcome gate keys on.
    ///
    /// CUMULATIVE: reads the whole out dir, so a second call after more runs
    /// returns old parts too — poll loops must ASSIGN the result, never `+=`
    /// it (bughunt: a `rows +=` poll double-counted one part and masked a
    /// dropped event).
    ///
    /// Refuses cloud/stdout rigs loudly: the data lands in the bucket (or on
    /// stdout), the local out dir is empty by construction, and the returned
    /// `[]` is indistinguishable from "the run wrote zero rows" — every
    /// negative assertion downstream would be vacuous forever.
    pub fn run_and_read(&self) -> Vec<arrow::record_batch::RecordBatch> {
        assert!(
            self.cloud_dest.is_none() && !self.dest_stdout,
            "run_and_read reads the rig's LOCAL out dir, but this rig writes to \
             a cloud/stdout destination — read the destination store directly \
             (mc/HTTP oracle), or drop the cloud dest"
        );
        self.run_ok();
        read_all_parts(&self.out_dir())
    }
}
