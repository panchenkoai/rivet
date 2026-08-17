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

    pub fn out_dir(&self) -> PathBuf {
        self.dest_override
            .clone()
            .unwrap_or_else(|| self.dir.path().join("out"))
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

    /// Destination directory of a named export — the primary or any secondary.
    pub fn out_dir_for(&self, name: &str) -> PathBuf {
        if name == self.name {
            return self.out_dir();
        }
        self.dir.path().join(format!("out_{name}"))
    }

    /// Run an ARBITRARY subcommand against this rig's config: `rivet <args…>
    /// --config <cfg>`.
    ///
    /// NOT for `apply`, which takes a PLAN PATH rather than `--config` — that is
    /// [`Rig::apply_env`]'s job. This method appends the config flag, so it
    /// fits the subcommands that read one: `plan`, `check`, `validate`, `doctor`.
    ///
    /// `run_args`/`run_args_env` hard-code the `run` subcommand, so a test for
    /// `check`, `validate`, `doctor` or `init` had no way through the rig and
    /// dropped to a raw `Command`. The config flag is appended, which clap
    /// accepts in any position.
    pub fn cli(&self, args: &[&str]) -> std::process::Output {
        self.cli_env(args, &[])
    }

    /// [`Rig::cli`] with environment variables — needed wherever the config
    /// declares `url_env:`, since the process must be able to resolve it.
    pub fn cli_env(&self, args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
        let cfg = self.config_path();
        let mut all: Vec<&str> = args.to_vec();
        all.push("--config");
        all.push(cfg.to_str().unwrap());
        super::runner::run_rivet_env(&all, envs)
    }

    /// `rivet plan --export <this rig's export> --format json --output <out>`,
    /// plus `extra` args (`--param k=v`, `--annotate-waves`, …).
    ///
    /// The plan→apply pair is the one CLI flow whose two halves take DIFFERENT
    /// subjects — `plan` reads the config, `apply` reads the artifact — so a
    /// test that wants the round trip had to spell the six-flag `plan`
    /// invocation itself and then drop out of the rig entirely for `apply`
    /// (every call site in `live_plan_apply.rs` does exactly that). The export
    /// name comes from the rig rather than the caller, which is what keeps the
    /// artifact, the destination and the `export_metrics` rows talking about the
    /// same export.
    pub fn plan_json_env(
        &self,
        out: &Path,
        extra: &[&str],
        envs: &[(&str, &str)],
    ) -> std::process::Output {
        let out = out.to_str().expect("plan output path must be utf-8");
        let mut args: Vec<&str> = vec![
            "plan",
            "--export",
            self.name.as_str(),
            "--format",
            "json",
            "--output",
            out,
        ];
        args.extend_from_slice(extra);
        self.cli_env(&args, envs)
    }

    /// `rivet apply <plan.json>` plus `extra` args (`--force`, `--resume`), with
    /// `envs` set — the counterpart of [`Rig::plan_json_env`].
    ///
    /// The ONE subcommand that takes a PLAN PATH instead of `--config`, which is
    /// why it cannot go through [`Rig::cli_env`] (that appends `--config`) and
    /// why it needs its own method rather than a raw `Command`. It still belongs
    /// on the rig: `apply` writes into the rig's destination and opens
    /// `.rivet_state.db` next to the rig's CONFIG (the artifact records the
    /// config path), so the read-backs a test does afterwards — `out_dir()`,
    /// the state DB — are the rig's, not the plan file's.
    ///
    /// `envs` is not optional in practice: a plan/apply round trip needs
    /// [`Rig::source_url_env`] (an inline URL is redacted into the artifact and
    /// apply then cannot reconnect), so the variable must be set on BOTH legs.
    pub fn apply_env(
        &self,
        plan: &Path,
        extra: &[&str],
        envs: &[(&str, &str)],
    ) -> std::process::Output {
        let mut args: Vec<&str> = vec!["apply", plan.to_str().expect("plan path must be utf-8")];
        args.extend_from_slice(extra);
        super::runner::run_rivet_env(&args, envs)
    }

    /// Spawn `rivet run` and hand back the LIVE child, output discarded.
    ///
    /// For tests that must act on a running process — signal it, inspect its
    /// children, watch the staged `.tmp` appear — rather than wait for an exit
    /// status. `run_args_env` blocks until completion and so cannot express them.
    /// The caller owns the `Child` and must reap it.
    pub fn spawn_args_env(&self, extra: &[&str], envs: &[(&str, &str)]) -> std::process::Child {
        let cfg = self.config_path();
        let mut cmd = std::process::Command::new(super::runner::RIVET_BIN);
        cmd.args(["run", "--config", cfg.to_str().unwrap()]);
        cmd.args(extra);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rivet")
    }

    /// Run `rivet run --config <rig cfg>` plus `extra` args, with `envs` set.
    ///
    /// The affordance the crash-recovery files were bypassing the rig for: they
    /// built their YAML through `Rig` and then dropped to a raw
    /// `Command::new(RIVET_BIN)` because the rig could express an env var OR a
    /// config, never extra ARGS (`--export`, `--resume`) alongside a fault
    /// injection. That one gap accounted for most of the hand-rolled invocations
    /// in `live_chunked_recovery.rs` and its siblings.
    pub fn run_args_env(&self, extra: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
        let cfg = self.config_path();
        let mut args: Vec<&str> = vec!["run", "--config", cfg.to_str().unwrap()];
        args.extend_from_slice(extra);
        super::runner::run_rivet_env(&args, envs)
    }

    /// `run_args_env` with no env — a plain run with extra flags.
    pub fn run_args(&self, extra: &[&str]) -> std::process::Output {
        self.run_args_env(extra, &[])
    }

    /// Run with an extra environment variable (fault injection); returns the
    /// raw output — the caller asserts success or failure.
    pub fn run_with_env(&self, key: &str, val: &str) -> std::process::Output {
        let cfg = self.config_path();
        super::runner::run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[(key, val)])
    }

    /// Run with SEVERAL extra environment variables (e.g. RIVET_STATE_URL to pick the
    /// Postgres state backend AND RIVET_TEST_PANIC_AT to inject a crash in one run);
    /// returns the raw output — the caller asserts success or failure.
    pub fn run_with_envs(&self, envs: &[(&str, &str)]) -> std::process::Output {
        let cfg = self.config_path();
        super::runner::run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], envs)
    }

    /// [`Rig::run_with_envs`] under a WALL-CLOCK CEILING — `None` if the child
    /// had to be killed.
    ///
    /// `run_with_envs` bottoms out in `Command::output()`, which blocks with no
    /// timeout. That is fine for a test whose failure mode is a wrong value, and
    /// wrong for one whose failure mode is a HANG: the governor deadlock
    /// (`governor_does_not_deadlock_when_chunks_fail`) is a live regression
    /// class, and a test that hangs while holding `quiet_window_guard` converts
    /// one red test into an indefinite stall of every test that takes the same
    /// cross-process lock — plus, for the pressure tests, a background writer
    /// that keeps hammering the shared server forever.
    ///
    /// stdout/stderr go to FILES rather than pipes: polling `try_wait` while a
    /// child fills a pipe buffer nobody drains is its own deadlock (the reason
    /// the hand-rolled watchdogs in `live_governor.rs` redirect to a file).
    pub fn run_with_envs_bounded(
        &self,
        envs: &[(&str, &str)],
        timeout: std::time::Duration,
    ) -> Option<std::process::Output> {
        let cfg = self.config_path();
        let out_path = self.dir.path().join("bounded.stdout");
        let err_path = self.dir.path().join("bounded.stderr");
        let mut cmd = std::process::Command::new(RIVET_BIN);
        cmd.args(["run", "--config", cfg.to_str().unwrap()]);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        cmd.stdout(std::fs::File::create(&out_path).expect("bounded stdout file"))
            .stderr(std::fs::File::create(&err_path).expect("bounded stderr file"));
        let mut child = cmd.spawn().expect("spawn rivet binary");
        let start = std::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("try_wait rivet") {
                return Some(std::process::Output {
                    status,
                    stdout: std::fs::read(&out_path).unwrap_or_default(),
                    stderr: std::fs::read(&err_path).unwrap_or_default(),
                });
            }
            if start.elapsed() >= timeout {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    pub fn checkpoint(&self) -> PathBuf {
        self.ckpt_override
            .clone()
            .unwrap_or_else(|| self.dir.path().join("cdc.ckpt"))
    }

    /// Override the checkpoint path (resume/crash suites share one
    /// checkpoint across several configs — the rig renders, the test owns
    /// the file's lifetime).
    pub fn checkpoint_path(mut self, path: PathBuf) -> Self {
        self.ckpt_override = Some(path);
        self
    }

    /// Output format (`csv`); parquet is the default.
    pub fn with_format(mut self, fmt: &'static str) -> Self {
        self.format = fmt;
        self
    }

    /// The rendered YAML — for suites that own their config-file lifetime.
    pub fn yaml(&self) -> String {
        self.render()
    }

    /// Materialized config path — for bespoke invocations (`validate`,
    /// custom envs) the rig doesn't wrap.
    /// The export name this rig rendered into the config — the key
    /// `export_metrics` rows are stored under, so a test can read the run's own
    /// metrics without re-deriving the name it did not choose.
    pub fn export_name(&self) -> &str {
        &self.name
    }

    pub fn config_path(&self) -> PathBuf {
        // Materialization point: the ONLY place the rig touches the
        // filesystem (yaml()/render() stay pure — the offline goldens were
        // mkdir-ing /tmp/o as a side effect of rendering a string).
        if self.dest_precreate {
            std::fs::create_dir_all(self.out_dir()).unwrap();
        }
        for e in &self.extra_exports {
            std::fs::create_dir_all(self.out_dir_for(&e.name)).unwrap();
        }
        let cfg = self.dir.path().join("rig.yaml");
        std::fs::write(&cfg, self.render()).unwrap();
        cfg
    }

    fn render(&self) -> String {
        let tables = match &self.query {
            Some(q) => format!("query: \"{q}\""),
            None if self.tables.len() == 1 => format!("table: {}", self.tables[0]),
            None => format!("tables: [{}]", self.tables.join(", ")),
        };
        let cdc_lines: Vec<String> = self
            .cdc_lines
            .iter()
            .map(|l| {
                if l == "__CKPT__" {
                    format!("checkpoint: \"{}\"", self.checkpoint().display())
                } else {
                    l.clone()
                }
            })
            .collect();
        let cdc = if cdc_lines.is_empty() {
            String::new()
        } else {
            format!("    cdc: {{ {} }}\n", cdc_lines.join(", "))
        };
        let extra: String = self
            .extra_lines
            .iter()
            .map(|l| format!("    {l}\n"))
            .collect();
        let source = if self.source_lines.is_empty() {
            match &self.url_env {
                Some(v) => format!("source: {{ type: {}, url_env: {v} }}", self.source_type),
                None => format!(
                    "source: {{ type: {}, url: \"{}\" }}",
                    self.source_type, self.source_url
                ),
            }
        } else {
            let extra: String = self
                .source_lines
                .iter()
                .map(|l| format!("  {l}\n"))
                .collect();
            match &self.url_env {
                Some(v) => format!(
                    "source:\n  type: {}\n  url_env: {v}\n{extra}",
                    self.source_type
                ),
                None => format!(
                    "source:\n  type: {}\n  url: \"{}\"\n{extra}",
                    self.source_type, self.source_url
                ),
            }
            .trim_end()
            .to_string()
        };
        // Secondary exports (see `Rig::also_export`) each get their OWN
        // destination, which is what the multi-export configs under test
        // actually declare — per-export failure isolation is only observable
        // when the outputs are separable.
        let secondaries: String = self
            .extra_exports
            .iter()
            .map(|e| {
                let lines: String = e.lines.iter().map(|l| format!("    {l}\n")).collect();
                let subject = match &e.table {
                    Some(t) => format!("table: {t}"),
                    None => format!("query: \"{}\"", e.query),
                };
                let cdc = if e.cdc_lines.is_empty() {
                    String::new()
                } else {
                    format!("    cdc: {{ {} }}\n", e.cdc_lines.join(", "))
                };
                format!(
                    "  - name: {n}\n    {subject}\n    mode: {m}\n    format: {f}\n{cdc}{lines}    destination: {{ type: local, path: \"{o}\" }}\n",
                    n = e.name,
                    m = e.mode,
                    f = self.format,
                    o = self.out_dir_for(&e.name).display(),
                )
            })
            .collect();
        let yaml = format!(
            "{source}\nexports:\n  - name: {name}\n    {tables}\n    mode: {mode}\n    format: {fmt}\n{cdc}{extra}    destination: {{ type: local, path: \"{out}\" }}\n{secondaries}",
            name = self.name,
            tables = tables,
            mode = self.mode,
            fmt = self.format,
            out = self.out_dir().display(),
        );
        yaml
    }

    /// Run rivet; panic unless it succeeds.
    pub fn run_ok(&self) {
        let cfg = self.config_path();
        let out = super::runner::run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
        assert!(
            out.status.success(),
            "rig run failed for '{}':\n{}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Run rivet expecting a loud failure; returns combined output.
    pub fn run_expect_fail(&self) -> String {
        let cfg = self.config_path();
        let out = super::runner::run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
        assert!(
            !out.status.success(),
            "rig run for '{}' was expected to fail",
            self.name
        );
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }

    /// Run rivet; return the raw output without asserting either way — for tests
    /// whose VALID outcomes include both success and a loud failure (e.g. a
    /// mid-stream outage that rivet may either retry through or safely refuse).
    pub fn run(&self) -> std::process::Output {
        let cfg = self.config_path();
        super::runner::run_rivet(&["run", "--config", cfg.to_str().unwrap()])
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

    /// Run and read every parquet part back — the canonical
    /// capture-and-verify shape the outcome gate keys on.
    pub fn run_and_read(&self) -> Vec<arrow::record_batch::RecordBatch> {
        self.run_ok();
        read_all_parts(&self.out_dir())
    }
}

/// Read every parquet part under `dir` (non-recursive), in filename order.
pub fn read_all_parts(dir: &Path) -> Vec<arrow::record_batch::RecordBatch> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
                .collect()
        })
        .unwrap_or_default();
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
        let tguard = super::pg::PgTable::adopt(table.clone());
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
