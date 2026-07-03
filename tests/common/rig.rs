//! The canonical live-test rig: ONE way to build a config, run rivet, and
//! read the output back. Replaces ~250 hand-rolled YAML templates and ~240
//! inline `Command::new(RIVET_BIN)` sites (measured before the
//! standardization pass) — and gives the conformance gate a single marker:
//! a capture test is one that calls `Rig::run*`, an outcome is read via
//! `read_*`/`run_and_read`.

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
    mode: &'static str,
    format: &'static str,
    cdc_lines: Vec<String>,
    extra_lines: Vec<String>,
    dir: tempfile::TempDir,
}

impl Rig {
    fn new(source_type: &'static str, url: &str, table: &str) -> Self {
        Self {
            source_type,
            source_url: url.to_string(),
            name: table.to_string(),
            tables: vec![table.to_string()],
            mode: "full",
            format: "parquet",
            cdc_lines: Vec::new(),
            extra_lines: Vec::new(),
            dir: tempfile::tempdir().expect("rig tempdir"),
        }
    }

    pub fn mysql_cdc(table: &str) -> Self {
        let mut r = Self::new("mysql", super::env::MYSQL_CDC_URL, table);
        r.mode = "cdc";
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push(format!(
            "checkpoint: \"{}\"",
            r.dir.path().join("cdc.ckpt").display()
        ));
        r.cdc_lines
            .push(format!("server_id: {}", server_id_for(table)));
        r
    }

    pub fn pg_cdc(table: &str, slot: &str) -> Self {
        let mut r = Self::new("postgres", super::env::POSTGRES_CDC_URL, table);
        r.mode = "cdc";
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push(format!("slot: {slot}"));
        r
    }

    pub fn mysql_batch(table: &str) -> Self {
        Self::new("mysql", super::env::MYSQL_CDC_URL, table)
    }

    /// Add tables to a multi-table CDC export.
    pub fn tables(mut self, tables: &[&str]) -> Self {
        self.tables = tables.iter().map(|t| t.to_string()).collect();
        self
    }

    /// Extra `cdc:` map entries, e.g. `initial: snapshot`.
    pub fn cdc(mut self, line: &str) -> Self {
        self.cdc_lines.push(line.to_string());
        self
    }

    /// Extra export-level lines verbatim (`columns: {..}`, `chunk_size: 5000`).
    pub fn export_line(mut self, line: &str) -> Self {
        self.extra_lines.push(line.to_string());
        self
    }

    pub fn out_dir(&self) -> PathBuf {
        self.dir.path().join("out")
    }

    pub fn checkpoint(&self) -> PathBuf {
        self.dir.path().join("cdc.ckpt")
    }

    /// Materialized config path — for bespoke invocations (`validate`,
    /// custom envs) the rig doesn't wrap.
    pub fn config_path(&self) -> PathBuf {
        let out = self.out_dir();
        std::fs::create_dir_all(&out).unwrap();
        let tables = if self.tables.len() == 1 {
            format!("table: {}", self.tables[0])
        } else {
            format!("tables: [{}]", self.tables.join(", "))
        };
        let cdc = if self.cdc_lines.is_empty() {
            String::new()
        } else {
            format!("    cdc: {{ {} }}\n", self.cdc_lines.join(", "))
        };
        let extra: String = self
            .extra_lines
            .iter()
            .map(|l| format!("    {l}\n"))
            .collect();
        let yaml = format!(
            "source: {{ type: {st}, url: \"{url}\" }}\nexports:\n  - name: {name}\n    {tables}\n    mode: {mode}\n    format: {fmt}\n{cdc}{extra}    destination: {{ type: local, path: \"{out}\" }}\n",
            st = self.source_type,
            url = self.source_url,
            name = self.name,
            tables = tables,
            mode = self.mode,
            fmt = self.format,
            out = self.out_dir().display(),
        );
        let cfg = self.dir.path().join("rig.yaml");
        std::fs::write(&cfg, yaml).unwrap();
        cfg
    }

    /// Run rivet; panic unless it succeeds.
    pub fn run_ok(&self) {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", self.config_path().to_str().unwrap()])
            .status()
            .expect("spawn rivet");
        assert!(st.success(), "rig run failed for '{}'", self.name);
    }

    /// Run rivet expecting a loud failure; returns combined output.
    pub fn run_expect_fail(&self) -> String {
        let out = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", self.config_path().to_str().unwrap()])
            .output()
            .expect("spawn rivet");
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
