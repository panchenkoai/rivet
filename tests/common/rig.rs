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
    mode: &'static str,
    format: &'static str,
    cdc_lines: Vec<String>,
    extra_lines: Vec<String>,
    dest_override: Option<PathBuf>,
    ckpt_override: Option<PathBuf>,
    dir: tempfile::TempDir,
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
            mode: "full",
            format: "parquet",
            cdc_lines: Vec::new(),
            extra_lines: Vec::new(),
            dest_override: None,
            ckpt_override: None,
            dir: tempfile::tempdir().expect("rig tempdir"),
        }
    }

    pub fn mysql_cdc(table: &str) -> Self {
        let mut r = Self::new("mysql", super::env::MYSQL_CDC_URL, table);
        r.mode = "cdc";
        r.cdc_lines.push("until_current: true".into());
        r.cdc_lines.push("__CKPT__".into()); // resolved at render time
        r.cdc_lines
            .push(format!("server_id: {}", server_id_for(table)));
        r
    }

    pub fn mssql_cdc(table: &str, capture_instance: &str) -> Self {
        let mut r = Self::new("mssql", super::env::MSSQL_CDC_URL, table);
        r.mode = "cdc";
        r.cdc_lines
            .push(format!("capture_instance: {capture_instance}"));
        r.cdc_lines.push("__CKPT__".into()); // resolved at render time
        r
    }

    pub fn pg_cdc(table: &str, slot: &str) -> Self {
        let mut r = Self::new("postgres", super::env::POSTGRES_CDC_URL, table);
        r.mode = "cdc";
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

    pub fn mssql_batch(table: &str) -> Self {
        let mut r = Self::new("mssql", super::env::MSSQL_URL, table);
        // The test stack's SQL Server runs a self-signed cert — every mssql
        // config needs the opt-in or the connection handshake fails.
        r.source_lines.push("tls:".into());
        r.source_lines.push("  accept_invalid_certs: true".into());
        r
    }

    /// Export mode (`full` / `incremental` / `chunked`); CDC constructors
    /// set `cdc`.
    pub fn mode(mut self, mode: &'static str) -> Self {
        self.mode = mode;
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
    pub fn source_url(mut self, url: &str) -> Self {
        self.source_url = url.to_string();
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

    /// Run with an extra environment variable (fault injection); returns the
    /// raw output — the caller asserts success or failure.
    pub fn run_with_env(&self, key: &str, val: &str) -> std::process::Output {
        std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", self.config_path().to_str().unwrap()])
            .env(key, val)
            .output()
            .expect("spawn rivet")
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
    pub fn config_path(&self) -> PathBuf {
        let cfg = self.dir.path().join("rig.yaml");
        std::fs::write(&cfg, self.render()).unwrap();
        cfg
    }

    fn render(&self) -> String {
        let out = self.out_dir();
        std::fs::create_dir_all(&out).unwrap();
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
            format!(
                "source: {{ type: {}, url: \"{}\" }}",
                self.source_type, self.source_url
            )
        } else {
            let extra: String = self
                .source_lines
                .iter()
                .map(|l| format!("  {l}\n"))
                .collect();
            format!(
                "source:\n  type: {}\n  url: \"{}\"\n{extra}",
                self.source_type, self.source_url
            )
            .trim_end()
            .to_string()
        };
        let yaml = format!(
            "{source}\nexports:\n  - name: {name}\n    {tables}\n    mode: {mode}\n    format: {fmt}\n{cdc}{extra}    destination: {{ type: local, path: \"{out}\" }}\n",
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

/// Run rivet against an explicit config path; panic with stderr on failure.
/// (One home for the run_cdc/run_ok copies four suites grew.)
pub fn run_rivet_ok(cfg: &Path) {
    let out = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
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

#[cfg(test)]
mod rig_render_goldens {
    use super::*;

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
