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

    /// Point the NEXT run at a fresh destination, so a resume leg's oracle cannot be
    /// satisfied by the crashed run's own parts.
    ///
    /// The CDC audit's headline finding: a crash test that shares one destination
    /// between the run that died and the run that resumes is satisfied by orphans.
    /// Parts are durable BEFORE the fault fires — `roll_all` flushes, the hook panics,
    /// and only then does save+ack run — so a resume that captures ZERO still reads the
    /// crashed run's parquet back and passes. Measured on Mongo, whose only
    /// at-least-once evidence is exactly that shape; PG/MySQL/MSSQL escape it only
    /// because their tests happen to hand-roll a second `out2/` directory.
    ///
    /// Hand-rolling is why this belongs on the rig rather than in each test: the three
    /// engines that got it right did so independently, and the one that did not looks
    /// identical at the call site.
    ///
    /// Returns the new directory so a caller can read the two legs separately.
    pub fn resume_into_fresh_dest(&mut self) -> PathBuf {
        // The leg number is derived from what is on disk rather than carried as
        // state: two calls in one test must not collide, and a rig that never
        // resumed must still get `out2`.
        // A shared-workdir pair, not a tempdir path: the DuckDB oracle reads from
        // INSIDE a container, so a resume destination it cannot see would make every
        // assertion downstream read zero — the failure mode that looks like data loss
        // and is a harness bug. `duckdb_oracle` sets both halves for the same reason;
        // the first cut of this seam set only the host half and the oracle came back
        // empty, which is how this comment exists.
        let (host, container) = super::super::env::live_shared_workdir(&super::super::unique_name(
            &format!("rig_{}_resume", self.name),
        ));
        self.dest_override = Some(host.clone());
        self.oracle_container_dir = Some(container);
        host
    }

    /// Read back ONLY the parts the manifest DECLARES, not every parquet under the dir.
    ///
    /// A glob answers "what does the destination hold"; every consumer — `rivet load`,
    /// `rivet validate`, reconcile — reads what the run DECLARED. On a crash cell the
    /// difference IS the test, because a crash leaves parts no manifest names. This
    /// existed on exactly one CDC engine (`manifest_driven_int_ids`, MySQL, one caller)
    /// while PG, MSSQL and Mongo globbed.
    ///
    /// Panics on a missing directory rather than returning `[]`: "no manifest" and "the
    /// run delivered nothing" must not look alike.
    pub fn read_declared_parts(&self) -> Vec<arrow::record_batch::RecordBatch> {
        let dir = self.out_dir();
        assert!(
            dir.is_dir(),
            "read_declared_parts: {} is not a directory — a missing dir is a harness bug,              never zero parts",
            dir.display()
        );
        let mut declared: Vec<PathBuf> = Vec::new();
        let mut copies: Vec<PathBuf> = std::fs::read_dir(&dir)
            .expect("read the destination")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("manifest-") && n.ends_with(".json"))
            })
            .collect();
        copies.sort();
        if copies.is_empty() && dir.join("manifest.json").is_file() {
            copies.push(dir.join("manifest.json"));
        }
        for m in &copies {
            let text = std::fs::read_to_string(m).expect("read a manifest");
            let doc: serde_json::Value = serde_json::from_str(&text).expect("parse a manifest");
            for part in doc
                .get("parts")
                .and_then(|p| p.as_array())
                .unwrap_or(&vec![])
            {
                // A part the manifest lists but does not mark committed is not delivered
                // data; counting it would report an in-flight row as an outcome.
                let committed = part
                    .get("status")
                    .and_then(|s| s.as_str())
                    .is_none_or(|s| s == "committed");
                if !committed {
                    continue;
                }
                if let Some(name) = part.get("path").and_then(|p| p.as_str()) {
                    let cand = dir.join(name);
                    if cand.is_file() {
                        declared.push(cand);
                    }
                }
            }
        }
        declared.sort();
        declared.dedup();
        // Read each declared part through a per-part temp view so the module's one
        // reader stays the only place that knows how a part is decoded.
        declared
            .iter()
            .flat_map(|p| {
                let one = self.dir.path().join(format!(
                    "declared_{}",
                    p.file_name().and_then(|n| n.to_str()).unwrap_or("part")
                ));
                std::fs::create_dir_all(&one).expect("stage a declared part");
                let link = one.join(p.file_name().expect("part file name"));
                if !link.exists() {
                    std::fs::copy(p, &link).expect("stage a declared part");
                }
                read_all_parts(&one)
            })
            .collect::<Vec<_>>()
    }
}
