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
        // Uniqueness comes from `unique_name`'s pid+counter, so two calls in
        // one test cannot collide. (An older comment here described a
        // derive-from-disk `out2` scheme that was never implemented.)
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
        // Same refusal `run_and_read` carries: a cloud/stdout rig's LOCAL out
        // dir exists (dest_precreate) and holds no manifests, so this would
        // return `[]` — indistinguishable from "zero parts declared", the
        // exact vacuity the module header promises to refuse (harness audit,
        // 2026-08-29).
        assert!(
            self.cloud_dest.is_none() && !self.dest_stdout,
            "read_declared_parts reads the rig's LOCAL out dir, but this rig \
             writes to a cloud/stdout destination — read the destination store \
             directly (mc/HTTP oracle), or drop the cloud dest"
        );
        let dir = self.out_dir();
        assert!(
            dir.is_dir(),
            "read_declared_parts: {} is not a directory — a missing dir is a \
             harness bug, never zero parts",
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
            // SUCCESS manifests only — same loader rule as
            // `declared_parquet_parts` (a Failed/Interrupted manifest's parts
            // are gc candidates, not delivered data; live-proven on the keyset
            // refused-resume fixture, 2026-08-29).
            let ok = doc
                .get("status")
                .and_then(|s| s.as_str())
                .is_none_or(|s| s.eq_ignore_ascii_case("success"));
            if !ok {
                continue;
            }
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
                // Always overwrite: part names key off the stable run_id, so a
                // resume/retry can CHANGE the bytes under an unchanged name —
                // an `if !exists` guard would re-read the first call's bytes
                // (the stale-staging class stage_declared_for_duckdb already
                // clears first for).
                std::fs::copy(p, &link).expect("stage a declared part");
                read_all_parts(&one)
            })
            .collect::<Vec<_>>()
    }
}

impl Rig {
    /// [`Rig::duckdb_oracle`] plus the CONFIG in the shared workdir, which is what
    /// [`Rig::row_census`] needs and `duckdb_oracle` alone does not give.
    ///
    /// The state DB lives beside the config. `duckdb_oracle` moves only the
    /// DESTINATION into the container's view, so a census through it reaches the
    /// parquet and not the ledger — and reconciling rivet's counters against the
    /// artifacts is the half that catches a run recording more than it delivered.
    pub fn census_oracle(mut self) -> Self {
        self = self.duckdb_oracle();
        // The rig's OWN shared directory holds both: the config at its root and the
        // destination one level down. The first version put the config in the shared
        // ROOT — which every rig shares — so `rig.yaml` collided across tests and the
        // hand-edit guard fired. The container path gains the same `/out` segment.
        let base = self.dest_override.clone().expect("duckdb_oracle set it");
        self.config_dir_override = Some(base.clone());
        let dest = base.join("out");
        std::fs::create_dir_all(&dest).expect("census destination");
        self.dest_override = Some(dest);
        self.oracle_container_dir = self.oracle_container_dir.map(|c| format!("{c}/out"));
        self
    }

    /// The four-way row census for THIS rig — source, delivered parquet,
    /// `export_metrics.total_rows` and `file_log.row_count`, from one DuckDB session.
    ///
    /// On the rig rather than called loose from a test body, for the reason the rig
    /// exists at all: the first hand-rolled call needed a `live_shared_workdir`, two
    /// hand-built container paths and a moved config, and got the state DB's location
    /// wrong on the first try. None of that is the test's business — the rig already
    /// owns its workdir, its destination, its export name and its source.
    ///
    /// The ENGINE is derived from the rig's own `source_type` and URL, so this reaches
    /// every engine without the caller naming one: postgres and mysql through DuckDB's
    /// core scanners, SQL Server and MongoDB through the community `mssql` / `mongo`
    /// extensions the stand now pins (DuckDB 1.5.0 — no build exists below it).
    ///
    /// Requires [`Rig::census_oracle`], which puts the CONFIG in the shared workdir
    /// too: the state DB sits beside the config, and a census that can reach the
    /// parquet but not the ledger is exactly half of the comparison that matters.
    pub fn row_census(&self) -> super::super::duckdb::RowCensus {
        let container = self.oracle_container_dir.as_ref().expect(
            "row_census needs `.census_oracle()` — the DuckDB container reads \
                     from inside itself and cannot see a bare tempdir, and a bucket or \
                     directory that reads as empty is indistinguishable from an export \
                     that wrote nothing",
        );
        let engine = self.oracle_engine();
        // ONE table. A multi-table rig has N sub-prefixes and N source relations, so a
        // single count would compare a sum against one of them — refused rather than
        // silently answering about the first.
        let [table] = self.tables.as_slice() else {
            panic!(
                "row_census compares ONE source relation against ONE destination; this \
                 rig captures {:?}. Census each table's sub-prefix separately.",
                self.tables
            )
        };
        super::super::duckdb::duckdb_row_census(
            engine,
            self.oracle_database(),
            table,
            &format!("{container}/**/*.parquet"),
            // The ledger sits beside the CONFIG, one level above the destination.
            &format!("{}/.rivet_state.db", container.trim_end_matches("/out")),
            &self.name,
        )
    }

    /// Which DuckDB reader reaches this rig's source. Derived, never passed in — a
    /// hand-named engine is one more thing a copied test body can get wrong, and the
    /// wrong one reads zero rows, which looks exactly like an empty source.
    fn oracle_engine(&self) -> super::super::duckdb::OracleEngine {
        use super::super::duckdb::OracleEngine as E;
        // Port-discriminated for EVERY family, not just postgres: the readers
        // attach to the CDC-stand containers (mysql-cdc/mssql-cdc/mongo-rs), so
        // mapping a MAIN-stand rig onto them censuses the WRONG SERVER — SQL
        // engines fail loudly on the missing table, but a mongo_batch rig scans
        // an absent collection as EMPTY, the silent source-0 this method's own
        // doc warns about (harness audit, 2026-08-29). No main-stand reader is
        // wired yet, so the honest answer for those rigs is a refusal.
        match self.source_type {
            "postgres" if self.source_url.contains(":5434") => E::PostgresCdc,
            "postgres" => E::Postgres,
            "mysql" if self.source_url.contains(":3307") => E::MysqlCdc,
            "mssql" if self.source_url.contains(":1434") => E::MssqlCdc,
            "mongo" if self.source_url.contains(":27018") => E::MongoRs,
            other => panic!(
                "row_census has no DuckDB reader for this rig (source type `{other}`, \
                 url `{}`): only the CDC-stand instances (pg :5434, mysql :3307, \
                 mssql :1434, mongo-rs :27018) and main-stand postgres are wired. \
                 Censusing a main-stand rig through a CDC-stand reader would grade \
                 the wrong server — wire the reader before using row_census here.",
                self.source_url
            ),
        }
    }

    /// The database the source URL names — the last path segment, minus any query.
    fn oracle_database(&self) -> &str {
        self.source_url
            .rsplit('/')
            .next()
            .map(|s| s.split('?').next().unwrap_or(s))
            .filter(|s| !s.is_empty())
            .unwrap_or("rivet")
    }
}
