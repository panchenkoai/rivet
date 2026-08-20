//! The canonical live-test rig: ONE way to build a config, run rivet, and
//! read the output back. Replaces ~250 hand-rolled YAML templates and ~240
//! inline `Command::new(RIVET_BIN)` sites (measured before the
//! standardization pass). The conformance gate recognizes `Rig::run*` as its
//! capture markers; outcome read-backs stay diverse on purpose (the oracle
//! dictionary in cdc_conformance_gate.rs — measured live, not collapsible).

use std::path::{Path, PathBuf};

use super::env::server_id_for;
use super::runner::RIVET_BIN;

/// Builder for a single-export rivet config. Defaults: parquet, local
/// destination inside the rig's tempdir, `until_current` CDC runs.
/// One cloud destination per rig — the S3/GCS/Azure emulator triple the live
/// stack ships. Kept as an enum so `dest_yaml` stays the single renderer for
/// every backend (two renderers per backend is the drift the rig exists to
/// prevent).
mod invoke;
mod materialize;
mod oracle;
mod render;

enum CloudDest {
    S3 {
        bucket: String,
        prefix: String,
        endpoint: String,
    },
    Gcs {
        bucket: String,
        prefix: String,
        endpoint: String,
    },
    Azure {
        container: String,
        prefix: String,
    },
}

pub struct Rig {
    source_type: &'static str,
    source_url: String,
    name: String,
    tables: Vec<String>,
    query: Option<String>,
    source_lines: Vec<String>,
    mode: String,
    format: &'static str,
    cdc_lines: Vec<String>,
    extra_lines: Vec<String>,
    dest_override: Option<PathBuf>,
    /// Pre-create the destination directory at `config_path()` time. False only
    /// for [`Rig::unwritable_dest_path`], whose whole fixture is a destination
    /// that cannot exist.
    dest_precreate: bool,
    /// When set, the source declares `url_env: <name>` INSTEAD of an inline
    /// `url:`. See [`Rig::source_url_env`].
    url_env: Option<String>,
    /// Additional exports rendered after the primary one. See [`Rig::also_export`].
    extra_exports: Vec<SecondaryExport>,
    /// Container-visible twin of `dest_override`, set by [`Rig::duckdb_oracle`].
    oracle_container_dir: Option<String>,
    ckpt_override: Option<PathBuf>,
    /// Cloud destination override when the exports write to a bucket/container
    /// instead of the tempdir. See [`Rig::dest_s3`] / [`Rig::dest_gcs`] /
    /// [`Rig::dest_azure`].
    cloud_dest: Option<CloudDest>,
    /// `destination: { type: stdout }` — for dispatch tests whose subject is
    /// the stdout destination itself. See [`Rig::dest_stdout`].
    dest_stdout: bool,
    /// Caller-owned config copies produced by [`Rig::config_in`] — a
    /// sanctioned mutation re-renders every one of them (materialize.rs).
    materialized_copies: std::cell::RefCell<Vec<PathBuf>>,
    /// Every YAML this rig has materialized, so the hand-edit guard can tell
    /// "stale rig render" (fine to overwrite) from "foreign edit" (refused).
    past_renders: std::cell::RefCell<Vec<String>>,
    dir: tempfile::TempDir,
}

/// A non-primary export in a multi-export config, with its OWN destination.
///
/// The three affordances below exist because `live_cli_flags.rs` could not use
/// the rig AT ALL, and the reasons were structural rather than neglect: five of
/// its nineteen configs declare TWO exports (this), its tests drive `check` /
/// `validate` / `doctor` rather than `run` ([`Rig::cli`]), and its signal tests
/// need a LIVE child process to kill ([`Rig::spawn_args_env`]). A seam that
/// cannot express a third of a file is why that file grew its own harness.
#[derive(Clone)]
struct SecondaryExport {
    name: String,
    /// A secondary declared by QUERY (the batch shape). Mutually exclusive with
    /// `table` — CDC exports address a table, not a query.
    query: String,
    /// A secondary declared by TABLE, with its own `cdc:` block. SQL Server CDC
    /// needs this: the product REFUSES `tables:` on that engine ("its capture
    /// instances are per-table; use one cdc export per table"), so a multi-table
    /// SQL Server capture is N exports in one config, not one export over N
    /// tables — and `query:` cannot express any of them.
    table: Option<String>,
    cdc_lines: Vec<String>,
    mode: String,
    lines: Vec<String>,
}

impl Rig {
    fn new(source_type: &'static str, url: &str, table: &str) -> Self {
        Self {
            source_type,
            source_url: url.to_string(),
            name: table.to_string(),
            tables: vec![table.to_string()],
            query: None,
            source_lines: Vec::new(),
            mode: "full".to_string(),
            format: "parquet",
            cdc_lines: Vec::new(),
            extra_lines: Vec::new(),
            dest_override: None,
            dest_precreate: true,
            url_env: None,
            extra_exports: Vec::new(),
            oracle_container_dir: None,
            ckpt_override: None,
            dest_stdout: false,
            cloud_dest: None,
            materialized_copies: std::cell::RefCell::new(Vec::new()),
            past_renders: std::cell::RefCell::new(Vec::new()),
            dir: tempfile::tempdir().expect("rig tempdir"),
        }
    }

    pub fn mysql_cdc(table: &str) -> Self {
        let mut r = Self::new("mysql", super::env::MYSQL_CDC_URL, table);
        r.mode = "cdc".to_string();
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push("__CKPT__".into()); // resolved at render time
        r.cdc_lines
            .push(format!("server_id: {}", server_id_for(table)));
        r
    }

    pub fn mssql_cdc(table: &str, capture_instance: &str) -> Self {
        let mut r = Self::new("mssql", super::env::MSSQL_CDC_URL, table);
        r.mode = "cdc".to_string();
        r.cdc_lines
            .push(format!("capture_instance: {capture_instance}"));
        r.cdc_lines.push("__CKPT__".into()); // resolved at render time
        r
    }

    pub fn pg_cdc(table: &str, slot: &str) -> Self {
        let mut r = Self::new("postgres", super::env::POSTGRES_CDC_URL, table);
        r.mode = "cdc".to_string();
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push(format!("slot: {slot}"));
        r
    }

    /// Batch constructors — one per engine, main-stack URLs. `mode` defaults
    /// to `full`; switch flows flip it via [`Rig::mode`].
    pub fn mysql_batch(table: &str) -> Self {
        Self::new("mysql", super::env::MYSQL_URL, table)
    }

    pub fn pg_batch(table: &str) -> Self {
        Self::new("postgres", super::env::POSTGRES_URL, table)
    }

    /// SQL Server batch against the GOVERNOR instance (:1435).
    ///
    /// For the two concurrency-governor canaries only. They assert opposite
    /// things about `Log Flush Waits/sec (_Total)` — a server-wide counter — so
    /// on the shared `mssql` service both were decided by whichever sibling test
    /// happened to be committing. See `env::MSSQL_GOVERNOR_URL`.
    pub fn mssql_governor_batch(table: &str) -> Self {
        let mut r = Self::new("mssql", super::env::MSSQL_GOVERNOR_URL, table);
        r.source_lines.push("tls:".into());
        r.source_lines.push("  accept_invalid_certs: true".into());
        r
    }

    pub fn mssql_batch(table: &str) -> Self {
        let mut r = Self::new("mssql", super::env::MSSQL_URL, table);
        // The test stack's SQL Server runs a self-signed cert — every mssql
        // config needs the opt-in or the connection handshake fails.
        r.source_lines.push("tls:".into());
        r.source_lines.push("  accept_invalid_certs: true".into());
        r
    }

    /// Mongo batch (standalone :27017). The db varies per test — chain
    /// `.source_url(&MongoTest::url(PORT, &db))`.
    pub fn mongo_batch(table: &str) -> Self {
        Self::new("mongo", super::env::MONGO_URL, table)
    }

    /// Mongo CDC (change stream — replica set :27018), `until_current` + a
    /// checkpoint, like the SQL CDC constructors. Override the db with
    /// `.source_url(&MongoTest::url(PORT, &db))`.
    pub fn mongo_cdc(table: &str) -> Self {
        let mut r = Self::new("mongo", super::env::MONGO_RS_URL, table);
        r.mode = "cdc".to_string();
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push("__CKPT__".into()); // resolved at render time
        r
    }

    /// Export mode (`full` / `incremental` / `chunked`); CDC constructors
    /// set `cdc`.
    /// `String`, not `&'static str`: a mode derived at runtime is a real need —
    /// `live_plan_apply.rs` builds one from a mode BLOCK shared across its tests.
    pub fn mode(mut self, mode: &str) -> Self {
        self.mode = mode.to_string();
        self
    }

    /// Query-based export (replaces the `table:` shortcut in the render).
    pub fn query(mut self, sql: &str) -> Self {
        self.query = Some(sql.to_string());
        self
    }

    /// Add tables to a multi-table CDC export.
    pub fn tables(mut self, tables: &[&str]) -> Self {
        self.tables = tables.iter().map(|t| t.to_string()).collect();
        self
    }

    /// Point the source at a different URL (a toxiproxy front, a scout
    /// container) while keeping the engine's config shape.
    /// Append a raw line under `source:` (2-space indented by render) — e.g.
    /// `tuning:` blocks for governor/adaptive tests. The same mechanism
    /// `mssql_batch` uses internally for its TLS opt-in.
    pub fn source_line(mut self, line: &str) -> Self {
        self.source_lines.push(line.to_string());
        self
    }

    pub fn source_url(mut self, url: &str) -> Self {
        self.source_url = url.to_string();
        self
    }

    /// Extra `cdc:` map entries, e.g. `initial: snapshot`.
    pub fn cdc(mut self, line: &str) -> Self {
        self.cdc_lines.push(line.to_string());
        self
    }

    /// `source.mongo.*` options, e.g. `.mongo("page_size: 500, resume: true")`.
    pub fn mongo(mut self, opts: &str) -> Self {
        self.source_lines.push(format!("mongo: {{ {opts} }}"));
        self
    }

    /// Extra export-level lines verbatim (`columns: {..}`, `chunk_size: 5000`).
    pub fn export_line(mut self, line: &str) -> Self {
        self.extra_lines.push(line.to_string());
        self
    }

    /// Send every export to an S3-compatible bucket instead of the rig's local
    /// tempdir — the SAME rig, so a cloud test does not fork into a hand-rolled
    /// YAML builder (the ~250 templates the rig replaced came back one engine at
    /// a time exactly that way).
    ///
    /// The prefix is per-export: each export lands under `<prefix>/<export>/`, so
    /// a multi-export config (a `--split` giant plus its sibling) stays separable
    /// on the cloud the way it is locally. Credentials go through `*_env` — the
    /// caller passes them in the run env, never inline in a committed config.
    pub fn dest_s3(mut self, bucket: &str, prefix: &str, endpoint: &str) -> Self {
        self.cloud_dest = Some(CloudDest::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            endpoint: endpoint.to_string(),
        });
        self
    }

    /// The fake-gcs sibling of [`Rig::dest_s3`]: anonymous access against the
    /// emulator endpoint, same per-export prefix layout.
    /// Write to `destination: { type: stdout }` — no directory is created.
    pub fn dest_stdout(mut self) -> Self {
        self.dest_stdout = true;
        self.dest_precreate = false;
        self
    }

    pub fn dest_gcs(mut self, bucket: &str, prefix: &str, endpoint: &str) -> Self {
        self.cloud_dest = Some(CloudDest::Gcs {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            endpoint: endpoint.to_string(),
        });
        self
    }

    /// The azurite sibling: the well-known dev account via
    /// `RIVET_TEST_AZURITE_KEY` (pass it in the run env), same layout.
    pub fn dest_azure(mut self, container: &str, prefix: &str) -> Self {
        self.cloud_dest = Some(CloudDest::Azure {
            container: container.to_string(),
            prefix: prefix.to_string(),
        });
        self
    }

    /// Point the destination somewhere outside the rig's tempdir (e.g. a
    /// mounted tiny filesystem for ENOSPC scenarios).
    pub fn dest_path(mut self, path: PathBuf) -> Self {
        self.dest_override = Some(path);
        self
    }

    /// Point the destination at a path the rig must NOT create — the fixture
    /// shape for a test whose subject is a destination that CANNOT be written.
    ///
    /// [`Rig::dest_path`] is pre-created at [`Rig::config_path`] time, which is
    /// right for every run that wants its output back. A write-failure fixture
    /// is the exact inverse: `governor_does_not_deadlock_when_chunks_fail`
    /// points the local destination *under a regular file* so every chunk's
    /// `dest.write` hits ENOTDIR, and the rig's own `create_dir_all` panics on
    /// that path before rivet is ever spawned. Skipping the pre-create is the
    /// entire difference — without it the all-chunks-fail path (and with it the
    /// governor-deadlock regression) cannot be stated through the seam at all.
    pub fn unwritable_dest_path(mut self, path: PathBuf) -> Self {
        self.dest_override = Some(path);
        self.dest_precreate = false;
        self
    }

    /// Name the EXPORT independently of the table it reads.
    ///
    /// `Rig::<engine>_batch(t)` names the export after the table, which is right
    /// for the common case and wrong whenever a test needs a unique export name
    /// for isolation while reading a fixed table — `live_keyset.rs` does this in
    /// 23 places, driving runs with `--export <unique>` against `table: <fixed>`.
    /// Without this the config declares one name and the CLI asks for another,
    /// which fails at RUN time, not compile time.
    pub fn export_named(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Append a raw line inside the `cdc: { … }` block — for per-test CDC
    /// knobs the constructors don't set (`initial: snapshot`, a bespoke
    /// `server_id`). Same escape-hatch contract as [`Rig::export_line`].
    pub fn cdc_line(mut self, line: &str) -> Self {
        self.cdc_lines.push(line.to_string());
        self
    }

    /// Declare the source URL through an ENV VAR (`url_env:`) instead of inline.
    ///
    /// Load-bearing for plan/apply round-trips, not a style choice: an inline URL
    /// carries credentials, which are REDACTED into the plan artifact, so `rivet
    /// apply` then cannot reconnect. Every plan/apply test therefore needs this
    /// shape, which is why `live_plan_apply.rs` had its own config builder.
    pub fn source_url_env(mut self, var: &str) -> Self {
        self.url_env = Some(var.to_string());
        self
    }

    /// Declare a SECOND (third, …) export in the same config, with its own
    /// destination directory — reachable via [`Rig::out_dir_for`].
    ///
    /// Multi-export configs are how `--export` selection, per-export failure
    /// isolation and wave ordering get tested; a single-export rig cannot state
    /// those cases at all.
    pub fn also_export(mut self, name: &str, query: &str) -> Self {
        self.extra_exports.push(SecondaryExport {
            name: name.to_string(),
            query: query.to_string(),
            table: None,
            cdc_lines: Vec::new(),
            mode: "full".to_string(),
            lines: Vec::new(),
        });
        self
    }

    /// A second CDC export over its OWN table, with its own `cdc:` block and its
    /// own checkpoint file.
    ///
    /// Exists because SQL Server cannot express a multi-table capture any other
    /// way: `Config` bails with "`tables:` is not yet supported for SQL Server —
    /// its capture instances are per-table; use one cdc export per table
    /// (capture_instance each)". Every routing question on that engine therefore
    /// needs TWO exports, and until now the suite had none — the whole
    /// multi-table SQL Server CDC surface was untested, including the routing
    /// bug that once dropped 100% of events for 6 of 8 tables (audit 2026-08-17).
    ///
    /// `cdc` lines are rendered verbatim into the map; `checkpoint:` is supplied
    /// here rather than by the caller, because two CDC exports sharing one
    /// checkpoint file would silently overwrite each other's position.
    pub fn also_cdc_export(mut self, name: &str, table: &str, cdc: &[&str]) -> Self {
        let ckpt = self.dir.path().join(format!("cdc_{name}.ckpt"));
        let mut cdc_lines: Vec<String> = cdc.iter().map(|l| l.to_string()).collect();
        cdc_lines.push(format!("checkpoint: \"{}\"", ckpt.display()));
        self.extra_exports.push(SecondaryExport {
            name: name.to_string(),
            query: String::new(),
            table: Some(table.to_string()),
            cdc_lines,
            mode: "cdc".to_string(),
            lines: Vec::new(),
        });
        self
    }

    /// Extra export-level lines for the most recently added [`Rig::also_export`].
    pub fn also_export_line(mut self, line: &str) -> Self {
        self.extra_exports
            .last_mut()
            .expect("also_export_line needs a preceding also_export")
            .lines
            .push(line.to_string());
        self
    }

    // ── THE invocation seam ─────────────────────────────────────────────
    // Every rig method that reaches the rivet binary goes through
    // `invoke_command` — one place for cross-cutting execution concerns
    // (env, io, future timeouts/log capture), and ONE spelling family for
    // the CDC conformance gate to derive its capture markers from (the
    // hand-maintained marker dictionary drifted twice in a month; see the
    // gate's `derived_capture_markers`).

    /// Override the checkpoint path (resume/crash suites share one
    /// checkpoint across several configs — the rig renders, the test owns
    /// the file's lifetime).
    pub fn checkpoint_path(mut self, path: PathBuf) -> Self {
        self.ckpt_override = Some(path);
        // The checkpoint only reaches the config if the cdc block renders it.
        // mysql_cdc/mssql_cdc seed the marker in their constructors; pg_cdc is
        // slot-anchored and doesn't — so a caller-supplied checkpoint must add
        // it, or the path is silently ABSENT from the rendered config (bitten:
        // live_cdc's corrupt-checkpoint tests ran checkpoint-less and green-
        // failed on the wrong arm).
        if self.mode == "cdc" && !self.cdc_lines.iter().any(|l| l == "__CKPT__") {
            self.cdc_lines.push("__CKPT__".to_string());
        }
        self
    }

    /// Output format (`csv`); parquet is the default.
    pub fn with_format(mut self, fmt: &'static str) -> Self {
        self.format = fmt;
        self
    }

    /// Put the destination under the shared bind mount so the DuckDB validator
    /// container can read it, enabling [`Rig::assert_complete`].
    ///
    /// Needed because the rig's own tempdir is invisible inside the container.
    /// Call it at construction; a test that does not need an independent decoder
    /// should not pay the container round-trip.
    pub fn duckdb_oracle(mut self) -> Self {
        // The label MUST be unique per rig, not per table. Deriving it from
        // `self.name` collided immediately: five Mongo tests all export a
        // collection called `bench`, so they shared one directory, ran in
        // parallel, and read each other's parquet — DuckDB reported 19000 rows /
        // 14000 distinct where each expected 5000. `live_shared_workdir` also
        // CLEARS the directory it hands back, so they were deleting each other's
        // output as well. Loud, but only because the oracle is strict.
        let (host, container) =
            super::env::live_shared_workdir(&super::unique_name(&format!("rig_{}", self.name)));
        self.dest_override = Some(host);
        self.oracle_container_dir = Some(container);
        self
    }

    /// Assert the destination holds exactly `expected` rows, all distinct on
    /// `column` — read by DuckDB, which does NOT share rivet's parquet codec.
    ///
    /// `read_all_parts` / `total_parquet_rows` decode with the same crate rivet
    /// encodes with, so a fault in that shared path cancels out and the claim
    /// passes over corrupt output. This is the reader that does not.
    ///
    /// Requires [`Rig::duckdb_oracle`] at construction.
    pub fn assert_complete(&self, column: &str, expected: i64, what: &str) {
        let dir = self
            .oracle_container_dir
            .as_deref()
            .expect("assert_complete needs Rig::duckdb_oracle() at construction");
        super::duckdb::duckdb_assert_complete(dir, column, expected, what);
    }

    /// The container-visible destination path, for a bespoke DuckDB query.
    pub fn oracle_dir(&self) -> &str {
        self.oracle_container_dir
            .as_deref()
            .expect("oracle_dir needs Rig::duckdb_oracle() at construction")
    }
}

/// Read every parquet part under `dir` (non-recursive), in filename order.
pub fn read_all_parts(dir: &Path) -> Vec<arrow::record_batch::RecordBatch> {
    // A MISSING dir is a harness bug (wrong path, dest never created), not
    // "zero parts" — swallowing it made a wrong out_dir read as an empty
    // export and every is_empty()-style assertion vacuous.
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_all_parts: {} is unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let file = std::fs::File::open(&f).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            out.push(b.unwrap());
        }
    }
    out
}

/// Run rivet against an explicit config path; panic with stderr on failure.
/// (One home for the run_cdc/run_ok copies four suites grew.)
pub fn run_rivet_ok(cfg: &Path) {
    let out = super::runner::run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "rivet run failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `row_count` from the manifest under `out`.
pub fn manifest_rows(out: &Path) -> i64 {
    let body = std::fs::read_to_string(out.join("manifest.json")).expect("manifest.json");
    let m: serde_json::Value = serde_json::from_str(&body).unwrap();
    m["row_count"].as_i64().expect("row_count")
}

/// The single `.parquet` part under `dir`, read as one RecordBatch.
pub fn read_one_batch(dir: &Path) -> arrow::record_batch::RecordBatch {
    let part = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part");
    let f = std::fs::File::open(part).unwrap();
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .expect("a row")
        .unwrap()
}

/// A connection to the mysql-cdc instance (the boilerplate 18 call sites
/// hand-rolled).
pub fn cdc_conn() -> mysql::PooledConn {
    mysql::Pool::new(super::env::MYSQL_CDC_URL)
        .expect("pool")
        .get_conn()
        .expect("conn")
}

/// The CDC setup owner: unique table + connection + drop-guard + rig +
/// checkpoint PIN in one call — the ~50-times-copied preamble, with the
/// anchor-at-open discipline built in (a test cannot forget to pin; the
/// idle-anchor loss class stays unrepresentable in new tests).
pub struct CdcScenario {
    pub rig: Rig,
    pub table: String,
    exec: ScnExec,
    _guards: Vec<Box<dyn std::any::Any>>,
}

/// Engine-specific SQL executors for [`CdcScenario`].
enum ScnExec {
    MySql(mysql::PooledConn),
    Pg(Box<postgres::Client>),
    /// SQL Server churns through the shared sqlcmd helper.
    Mssql,
}

/// Drops a SQL Server capture instance + table on teardown (a CDC-tracked
/// table can't just be dropped — the change table would orphan).
pub struct MssqlCdcTable {
    pub table: String,
    pub ci: String,
}
impl Drop for MssqlCdcTable {
    fn drop(&mut self) {
        let (table, ci) = (self.table.clone(), self.ci.clone());
        let _ = std::panic::catch_unwind(move || {
            super::mssql::mssql_cdc_exec(&format!(
                "IF EXISTS(SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t \
                   ON ct.source_object_id=t.object_id WHERE t.name='{table}') \
                 EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', \
                 @source_name=N'{table}', @capture_instance=N'{ci}';"
            ));
            super::mssql::mssql_cdc_drop_table(&format!("dbo.{table}"));
        });
    }
}

impl CdcScenario {
    /// `cols` is the column spec, e.g. `"id INT PRIMARY KEY, v BIGINT"`.
    pub fn mysql(label: &str, cols: &str) -> Self {
        use mysql::prelude::Queryable as _;
        let table = super::unique_name(label);
        let mut conn = cdc_conn();
        conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .unwrap();
        conn.query_drop(format!("CREATE TABLE {table} ({cols})"))
            .unwrap();
        let guard = super::mysql::MysqlCdcTable(table.clone());
        let rig = Rig::mysql_cdc(&table);
        rig.run_ok(); // pin: the checkpoint anchors BEFORE any churn
        Self {
            rig,
            table,
            exec: ScnExec::MySql(conn),
            _guards: vec![Box::new(guard)],
        }
    }

    /// PostgreSQL: table + logical slot (both guarded) + pin.
    pub fn pg(label: &str, cols: &str) -> Self {
        let table = super::unique_name(label);
        let slot = super::unique_name(&format!("{label}_slot"));
        let mut client = postgres::Client::connect(super::env::POSTGRES_CDC_URL, postgres::NoTls)
            .expect("connect postgres-cdc");
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({cols})"
            ))
            .unwrap();
        let tguard = super::pg::PgTable::adopt_on(super::env::POSTGRES_CDC_URL, table.clone());
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )
            .unwrap();
        let sguard = super::pg::Slot(slot.clone());
        let rig = Rig::pg_cdc(&table, &slot);
        rig.run_ok(); // pin (PG anchors server-side at slot creation)
        Self {
            rig,
            table,
            exec: ScnExec::Pg(Box::new(client)),
            _guards: vec![Box::new(tguard), Box::new(sguard)],
        }
    }

    /// SQL Server: table + capture instance (guarded) + pin at max LSN.
    pub fn mssql(label: &str, cols: &str) -> Self {
        let table = super::unique_name(label);
        let ci = format!("dbo_{table}");
        super::mssql::mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} ({cols})"));
        super::mssql::mssql_cdc_exec(
            "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' \
              AND is_cdc_enabled=1) EXEC sys.sp_cdc_enable_db;",
        );
        super::mssql::mssql_cdc_exec(&format!(
            "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', \
             @source_name=N'{table}', @role_name=NULL, @capture_instance=N'{ci}';"
        ));
        let guard = MssqlCdcTable {
            table: table.clone(),
            ci: ci.clone(),
        };
        let rig = Rig::mssql_cdc(&table, &ci);
        rig.run_ok(); // pin
        Self {
            rig,
            table,
            exec: ScnExec::Mssql,
            _guards: vec![Box::new(guard)],
        }
    }

    /// Execute source-side SQL (churn) against the scenario's table.
    pub fn sql(&mut self, q: &str) {
        match &mut self.exec {
            ScnExec::MySql(conn) => {
                use mysql::prelude::Queryable as _;
                conn.query_drop(q).expect("scenario sql (mysql)");
            }
            ScnExec::Pg(client) => client.batch_execute(q).expect("scenario sql (pg)"),
            ScnExec::Mssql => super::mssql::mssql_cdc_exec(q),
        }
    }

    /// Drain the stream and read every part back.
    pub fn drain_and_read(&self) -> Vec<arrow::record_batch::RecordBatch> {
        self.rig.run_and_read()
    }
}

#[cfg(test)]
mod rig_render_goldens {
    use super::*;

    /// `url_env:` replaces the inline `url:` in both source-render shapes.
    ///
    /// Pins the affordance `live_plan_apply.rs` needed. Not cosmetic: an inline
    /// URL carries credentials, which are redacted into the plan artifact, so a
    /// subsequent `rivet apply` cannot reconnect. Every plan/apply test uses this
    /// shape — which is why that file had its own config builder rather than a
    /// stray preference for one.
    #[test]
    fn source_url_env_replaces_the_inline_url_in_both_render_shapes() {
        // Inline shape (postgres: no extra source lines).
        let flat = Rig::pg_batch("t")
            .source_url_env("DATABASE_URL")
            .dest_path("/tmp/o".into())
            .yaml();
        assert!(
            flat.contains("source: { type: postgres, url_env: DATABASE_URL }"),
            "inline source must carry url_env:\n{flat}"
        );
        assert!(
            !flat.contains("url: \""),
            "…and must NOT also emit an inline url, or the credential is back:\n{flat}"
        );

        // Multi-line shape (mssql: carries tls lines).
        let nested = Rig::mssql_batch("t")
            .source_url_env("MSSQL_URL")
            .dest_path("/tmp/o".into())
            .yaml();
        assert!(
            nested.contains("url_env: MSSQL_URL"),
            "multi-line source must carry url_env:\n{nested}"
        );
        assert!(
            !nested.contains("url: \""),
            "…and must not keep the inline url beside it:\n{nested}"
        );
        assert!(
            nested.contains("accept_invalid_certs: true"),
            "the engine's own source lines must survive:\n{nested}"
        );
    }

    /// A second export renders as its own block with its OWN destination.
    ///
    /// Pins the affordance that `live_cli_flags.rs` needed and the rig did not
    /// have: five of its nineteen configs declare two exports, so a third of
    /// that file could not go through the seam at all and grew a hand-rolled
    /// template instead.
    ///
    /// The destinations must DIFFER, which is the load-bearing half — per-export
    /// failure isolation and `--export` selection are only observable when the
    /// outputs are separable. The paths themselves live under the rig's tempdir
    /// and so are not pinned; that they are distinct is.
    #[test]
    fn a_second_export_renders_its_own_block_and_its_own_destination() {
        let rig = Rig::pg_batch("primary")
            .also_export("secondary", "SELECT id FROM other")
            .also_export_line("chunk_size: 10");
        let yaml = rig.yaml();

        assert_eq!(
            yaml.matches("- name:").count(),
            2,
            "both exports must render:\n{yaml}"
        );
        assert!(
            yaml.contains("- name: secondary"),
            "the secondary export is missing:\n{yaml}"
        );
        assert!(
            yaml.contains(r#"query: "SELECT id FROM other""#),
            "the secondary keeps its own query:\n{yaml}"
        );
        assert!(
            yaml.contains("    chunk_size: 10\n"),
            "also_export_line attaches to the secondary:\n{yaml}"
        );
        assert_ne!(
            rig.out_dir_for("primary"),
            rig.out_dir_for("secondary"),
            "each export needs a separable destination, or per-export failure \
             isolation cannot be observed"
        );
        let dests: Vec<&str> = yaml.matches("path: ").collect();
        assert_eq!(dests.len(), 2, "one destination per export:\n{yaml}");
    }

    /// The full constructor surface, pinned as rendered YAML — the rig's own
    /// contract test. Any drift in the render is a diff HERE first, offline,
    /// before any live suite meets it.
    #[test]
    fn every_engine_constructor_renders_the_pinned_shape() {
        let cases: [(&str, Rig, &str); 5] = [
            (
                "mysql_batch",
                Rig::mysql_batch("t").dest_path("/tmp/o".into()),
                "source: { type: mysql, url: \"mysql://rivet:rivet@127.0.0.1:3306/rivet\" }\nexports:\n  - name: t\n    table: t\n    mode: full\n    format: parquet\n    destination: { type: local, path: \"/tmp/o\" }\n",
            ),
            (
                "pg_batch",
                Rig::pg_batch("t").dest_path("/tmp/o".into()),
                "source: { type: postgres, url: \"postgresql://rivet:rivet@127.0.0.1:5432/rivet\" }\nexports:\n  - name: t\n    table: t\n    mode: full\n    format: parquet\n    destination: { type: local, path: \"/tmp/o\" }\n",
            ),
            (
                "mssql_batch",
                Rig::mssql_batch("t").dest_path("/tmp/o".into()),
                "source:\n  type: mssql\n  url: \"sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet\"\n  tls:\n    accept_invalid_certs: true\nexports:\n  - name: t\n    table: t\n    mode: full\n    format: parquet\n    destination: { type: local, path: \"/tmp/o\" }\n",
            ),
            (
                "mysql_cdc",
                Rig::mysql_cdc("t")
                    .checkpoint_path("/tmp/ck".into())
                    .dest_path("/tmp/o".into()),
                "source: { type: mysql, url: \"mysql://rivet:rivet@127.0.0.1:3307/rivet\" }\nexports:\n  - name: t\n    table: t\n    mode: cdc\n    format: parquet\n    cdc: { until_current: true, checkpoint: \"/tmp/ck\", server_id: SID }\n    destination: { type: local, path: \"/tmp/o\" }\n",
            ),
            (
                "pg_cdc",
                Rig::pg_cdc("t", "s1").dest_path("/tmp/o".into()),
                "source: { type: postgres, url: \"postgresql://rivet:rivet@127.0.0.1:5434/rivet\" }\nexports:\n  - name: t\n    table: t\n    mode: cdc\n    format: parquet\n    cdc: { until_current: true, slot: s1 }\n    destination: { type: local, path: \"/tmp/o\" }\n",
            ),
        ];
        for (name, rig, want) in cases {
            let want = want.replace("SID", &super::super::env::server_id_for("t").to_string());
            assert_eq!(rig.yaml(), want, "constructor '{name}' drifted");
        }
    }
}

// ─── render goldens (offline; they run under the live_suite target with no DB) ───

/// Render goldens for the rig affordances this wave grew — the offline half of
/// the migration-equivalence protocol. A live run proves a rendered config
/// WORKS today; the golden pins what it SAYS, so a rig refactor that silently
/// changes a destination block (the exact risk of centralizing 72 files'
/// config-building into one renderer) fails here with a diff, not in a cloud
/// test's 404. Same shape as live_cdc's rig_renders_the_exact_legacy_cdc_template.
#[test]
fn cloud_dest_render_goldens() {
    let s3 = Rig::pg_batch("t")
        .export_named("e")
        .dest_s3("bkt", "pfx", "http://127.0.0.1:9000")
        .yaml();
    assert!(
        s3.contains(
            "destination: { type: s3, bucket: bkt, prefix: \"pfx/e/\", region: us-east-1, \
             endpoint: \"http://127.0.0.1:9000\", access_key_env: RIVET_TEST_MINIO_AK, \
             secret_key_env: RIVET_TEST_MINIO_SK }"
        ),
        "dest_s3 render drifted:\n{s3}"
    );
    let gcs = Rig::pg_batch("t")
        .export_named("e")
        .dest_gcs("bkt", "pfx", "http://127.0.0.1:4443")
        .yaml();
    assert!(
        gcs.contains(
            "destination: { type: gcs, bucket: bkt, prefix: \"pfx/e/\", \
             endpoint: \"http://127.0.0.1:4443\", allow_anonymous: true }"
        ),
        "dest_gcs render drifted:\n{gcs}"
    );
    let az = Rig::pg_batch("t")
        .export_named("e")
        .dest_azure("ctr", "pfx")
        .yaml();
    assert!(
        az.contains("type: azure, bucket: ctr, prefix: \"pfx/e/\"")
            && az.contains("account_key_env: RIVET_TEST_AZURITE_KEY"),
        "dest_azure render drifted:\n{az}"
    );
    let so = Rig::pg_batch("t").export_named("e").dest_stdout().yaml();
    assert!(
        so.contains("destination: { type: stdout }"),
        "dest_stdout render drifted:\n{so}"
    );
}

/// A hand-edited rig.yaml must be REFUSED, not silently clobbered by the
/// next run's re-render (bughunt: three resume tests measured nothing on
/// their changed-parallelism leg exactly this way).
#[test]
#[should_panic(expected = "edited outside the rig")]
fn config_path_refuses_a_hand_edited_config() {
    let rig = Rig::pg_batch("t").export_named("e");
    let cfg = rig.config_path();
    std::fs::write(&cfg, "hand-patched\n").unwrap();
    let _ = rig.config_path();
}

/// The sanctioned mutation path: replace_export_line changes the knob in
/// the BUILDER, so the re-render and the file agree.
#[test]
fn replace_export_line_changes_the_rendered_knob() {
    let mut rig = Rig::pg_batch("t")
        .export_named("e")
        .export_line("parallel: 1");
    let cfg = rig.config_path();
    rig.replace_export_line("parallel:", "parallel: 2");
    let text = std::fs::read_to_string(rig.config_path()).unwrap();
    assert!(text.contains("parallel: 2"), "knob must change: {text}");
    assert!(
        !text.contains("parallel: 1"),
        "old knob must be gone: {text}"
    );
    assert_eq!(cfg, rig.config_path(), "same config path throughout");
}

/// run_and_read on a cloud/stdout rig must refuse — the local out dir is
/// empty by construction and `[]` would make every negative assertion
/// vacuous forever.
#[test]
#[should_panic(expected = "LOCAL out dir")]
fn run_and_read_refuses_a_cloud_rig() {
    let rig = Rig::pg_batch("t")
        .export_named("e")
        .dest_gcs("bkt", "pfx", "http://127.0.0.1:4443");
    let _ = rig.run_and_read();
}

/// DOCUMENTS the absorb contract (r3 bughunt): a product write into the
/// config (rivet plan annotating a wave-less config) is absorbed — no
/// hand-edit panic — and then OVERWRITTEN by the next materialization,
/// because the builder is the single source of truth. A test that wants to
/// assert on product-written content must read the file BEFORE any further
/// rig call. This golden makes the silent overwrite a stated behavior, not
/// an accident.
#[test]
fn product_config_write_is_absorbed_then_overwritten_documents_the_contract() {
    let rig = Rig::pg_batch("t").export_named("e");
    let cfg = rig.config_path();
    let pristine = std::fs::read_to_string(&cfg).unwrap();
    // Simulate the product's write the way absorb sees it: foreign content on
    // disk, then an invocation completes (absorb runs).
    let annotated = format!("{pristine}    wave: 3\n");
    std::fs::write(&cfg, &annotated).unwrap();
    rig.absorb_product_config_writes();
    // No panic — absorbed. And the next materialization restores the render:
    let after = std::fs::read_to_string(rig.config_path()).unwrap();
    assert_eq!(after, pristine, "the builder's render wins after absorb");
}

/// A caller-owned `config_in` copy must FOLLOW a sanctioned mutation — the
/// r2 bughunt found amend/replace re-rendered only the rig-dir config, so the
/// caller's path kept executing yesterday's knobs while the builder (and the
/// test's text) looked amended.
#[test]
fn config_in_copy_follows_replace_export_line() {
    let dir = tempfile::tempdir().expect("caller dir");
    let mut rig = Rig::pg_batch("t")
        .export_named("e")
        .export_line("parallel: 1");
    let copy = rig.config_in(dir.path());
    rig.replace_export_line("parallel:", "parallel: 2");
    let text = std::fs::read_to_string(&copy).expect("caller copy");
    assert!(
        text.contains("parallel: 2") && !text.contains("parallel: 1"),
        "the config_in copy must be re-rendered by the mutation: {text}"
    );
}

/// `config_in` must write the SAME bytes `config_path` writes — the "two
/// materializations cannot drift" claim, asserted rather than commented.
#[test]
fn config_in_matches_config_path_byte_for_byte() {
    let rig = Rig::pg_batch("t").export_named("e");
    let own = std::fs::read_to_string(rig.config_path()).expect("own config");
    let dir = tempfile::tempdir().expect("caller dir");
    let theirs = std::fs::read_to_string(rig.config_in(dir.path())).expect("caller config");
    assert_eq!(
        own, theirs,
        "config_in and config_path rendered different configs"
    );
}
