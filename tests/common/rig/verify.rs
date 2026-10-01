//! VERIFY — the rig's DEFAULT independent oracle. After every `run` that exits
//! 0 the rig gathers FACTS (engine, source URL, table/query, the export's own
//! filter, the Success manifests the run wrote, the state DB) and hands them to
//! `dev/release_oracle/rig_oracle.py`, which owns the one DuckDB session and
//! every check. Opt out only with `.no_oracle("<reason>")`; a rig the oracle
//! cannot reach logs a `RIVET-ORACLE-SKIP` line, never silence.

use super::*;
use std::collections::BTreeSet;

/// Success manifest names in the destination and its `snapshot/` leg, taken before an invocation.
pub(crate) type ManifestSnapshot = [BTreeSet<String>; 2];

impl Rig {
    /// Opt this rig out of the default oracle; the reason is required and counted by an offline ceiling.
    pub fn no_oracle(mut self, reason: &str) -> Self {
        assert!(
            !reason.trim().is_empty(),
            "no_oracle needs a reason — say why this rig's output must not be graded"
        );
        self.oracle_off = Some(reason.to_string());
        self
    }

    /// A known product defect the oracle must keep catching: a disagreement is expected, and a rig whose graded runs all agree fails with "now passes".
    pub fn oracle_known_defect(mut self, reason: &str) -> Self {
        assert!(
            !reason.trim().is_empty(),
            "oracle_known_defect needs a reason — name the defect and the step that fixes it"
        );
        self.oracle_xfail = Some(reason.to_string());
        self
    }

    /// Snapshot the Success manifest names before a `run`, so the oracle can tell which ones this run wrote.
    pub(crate) fn oracle_before(&self) -> ManifestSnapshot {
        if self.oracle_off.is_some() || self.oracle_unreachable().is_some() {
            return Default::default();
        }
        // A bucket the test has not created yet holds no manifest.
        let Ok(out) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.oracle_local_out()))
        else {
            return Default::default();
        };
        [
            success_manifests(&out),
            success_manifests(&out.join("snapshot")),
        ]
    }

    /// The destination as a local directory: the out dir itself, or a MinIO prefix pulled whole (the same oracle then reads both).
    fn oracle_local_out(&self) -> PathBuf {
        match &self.cloud_dest {
            Some(CloudDest::S3 { bucket, prefix, .. }) => {
                let into = self
                    .dir
                    .path()
                    .join(super::super::unique_name("oracle_pull"));
                super::super::storage::minio_pull_prefix(
                    bucket,
                    &format!("{prefix}/{}/", self.name),
                    &into,
                );
                into
            }
            _ => self.out_dir(),
        }
    }

    /// Grade a successful `run` with the default oracle; panics with every disagreement it reports.
    pub(crate) fn oracle_after(
        &self,
        before: &ManifestSnapshot,
        envs: &[(&str, &str)],
        argv: &[String],
    ) {
        if self.oracle_off.is_some() {
            return;
        }
        if let Some(why) = self.oracle_unreachable() {
            return oracle_log("SKIP", &self.name, &why);
        }
        let cdc = self.mode == "cdc";
        let [seen, seen_snap] = before;
        let out = &self.oracle_local_out();
        let snap = &out.join("snapshot");
        let (now, now_snap) = (success_manifests(out), success_manifests(snap));
        if now.is_subset(seen) && now_snap.is_subset(seen_snap) {
            return oracle_log("SKIP", &self.name, "the run wrote no new Success manifest");
        }
        // CDC and delta modes grade the cumulative output; a snapshot run grades only what it declared.
        let cumulative = cdc || self.oracle_is_delta();
        let fresh = |all: &BTreeSet<String>, old: &BTreeSet<String>| -> Vec<String> {
            all.difference(old).cloned().collect()
        };
        let graded = |all: &BTreeSet<String>, old: &BTreeSet<String>| -> Vec<String> {
            if cumulative {
                all.iter().cloned().collect()
            } else {
                fresh(all, old)
            }
        };
        let mut spec = self.oracle_facts(envs, argv);
        let more = serde_json::json!({
            "cumulative": cumulative,
            "out_dir": out,
            "manifests": graded(&now, seen),
            "new_manifests": fresh(&now, seen),
            "snapshot_dir": snap,
            "snapshot_manifests": graded(&now_snap, seen_snap),
            "new_snapshot_manifests": fresh(&now_snap, seen_snap),
        });
        spec.as_object_mut()
            .expect("facts are an object")
            .extend(more.as_object().expect("an object").clone());
        self.oracle_verdict(&spec, "grade");
    }

    /// Grade the warehouse table a successful `rivet load`/`compact` left, against the source.
    pub(crate) fn oracle_after_load(&self, envs: &[(&str, &str)], argv: &[String]) {
        if self.oracle_off.is_some() {
            return;
        }
        if self.tables.len() > 1 {
            return oracle_log(
                "SKIP",
                &self.name,
                "load: a multi-table capture loads one table per source table, not graded yet",
            );
        }
        let top = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&self.top_lines.join("\n"))
            .unwrap_or_default();
        let Some(load) = top.get("load").filter(|l| l.is_mapping()) else {
            return oracle_log(
                "SKIP",
                &self.name,
                "load: no top-level `load:` block to read the target from",
            );
        };
        let env = |k: &str| {
            envs.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
                .or_else(|| std::env::var(k).ok())
        };
        let password = load
            .get("password_env")
            .and_then(|v| v.as_str())
            .and_then(env)
            .unwrap_or_default();
        let mut spec = self.oracle_facts(envs, argv);
        let more = serde_json::json!({
            "load": serde_json::to_value(load).unwrap_or_default(),
            "password": password,
            "export": self.name,
            "snapshot": self.cdc_lines.iter().any(|l| l.contains("initial: snapshot") || l.contains("backfill")),
        });
        spec.as_object_mut()
            .expect("facts are an object")
            .extend(more.as_object().expect("an object").clone());
        self.oracle_verdict(&spec, "grade-load");
    }

    /// The source facts every oracle verb needs: engine, URL, relation, key, overrides, cursor, state DB.
    fn oracle_facts(&self, envs: &[(&str, &str)], argv: &[String]) -> serde_json::Value {
        let state = envs
            .iter()
            .find(|(k, _)| *k == "RIVET_STATE_URL")
            .map(|(_, v)| v.to_string())
            .or_else(|| std::env::var("RIVET_STATE_URL").ok())
            .filter(|u| u.starts_with("postgres"))
            .or_else(|| {
                let db = self.config_dir().join(".rivet_state.db");
                db.is_file().then(|| db.display().to_string())
            });
        let url = oracle_source_url(&self.source_url);
        let lines = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&self.extra_lines.join("\n"))
            .unwrap_or_default();
        let overrides: Vec<String> = lines
            .get("columns")
            .and_then(|c| c.as_mapping())
            .map(|m| {
                m.keys()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let line = |k: &str| lines.get(k).and_then(|v| v.as_str()).map(str::to_string);
        let cursor_expr =
            (line("incremental_cursor_mode").as_deref() == Some("coalesce")).then(|| {
                format!(
                    "coalesce(\"{}\", \"{}\")",
                    line("cursor_column").unwrap_or_default(),
                    line("cursor_fallback_column").unwrap_or_default()
                )
            });
        serde_json::json!({
            "engine": self.source_type,
            "url": url,
            "database": url.rsplit('/').next().and_then(|s| s.split('?').next()).unwrap_or(""),
            "table": self.tables.first(),
            "query": self.query.as_deref().map(|q| rendered_query(q, argv)),
            "mode": if self.mode == "cdc" { "cdc" } else { "batch" },
            "key": self.census_key.iter().collect::<Vec<_>>(),
            "overrides": overrides,
            "cursor_expr": cursor_expr,
            "state": state,
            "capture_instance": self.cdc_lines.iter().find_map(|l| l.strip_prefix("capture_instance: ")),
        })
    }

    /// Run one oracle verb over `spec`; log PASS / SKIP / XFAIL, or panic with every disagreement.
    fn oracle_verdict(&self, spec: &serde_json::Value, verb: &str) {
        let t0 = std::time::Instant::now();
        let verdict = run_rig_oracle(spec, verb);
        let took = format!("{verb} {} ms", t0.elapsed().as_millis());
        if let Some(why) = verdict["skip"].as_str() {
            return oracle_log("SKIP", &self.name, &format!("{took}: {why}"));
        }
        let failures: Vec<String> = verdict["failures"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| f.as_str().map(str::to_string))
            .collect();
        if failures.is_empty() {
            return oracle_log("PASS", &self.name, &format!("{took} {}", verdict["facts"]));
        }
        if let Some(why) = &self.oracle_xfail {
            self.oracle_xfailed.set(true);
            return oracle_log(
                "XFAIL",
                &self.name,
                &format!("{why} — {}", failures.join(" | ")),
            );
        }
        oracle_log("FAIL", &self.name, &failures.join(" | "));
        panic!(
            "rig oracle: export '{}' disagrees with its source / rivet's own ledger \
             (dev/release_oracle/rig_oracle.py {verb}; opt out only with `.no_oracle(\"<why>\")`):\n  - {}\n\
             spec: {spec}\nverdict: {verdict}",
            self.name,
            failures.join("\n  - ")
        );
    }

    /// Why this rig's output cannot be graded by the default oracle, or `None` when it can.
    fn oracle_unreachable(&self) -> Option<String> {
        if self.format != "parquet" {
            return Some(format!(
                "format `{}`: the oracle grades parquet",
                self.format
            ));
        }
        if self.dest_stdout {
            return Some("stdout destination: nothing durable to read".into());
        }
        match &self.cloud_dest {
            None => {}
            Some(CloudDest::S3 { .. }) if self.mode == "cdc" => {
                return Some(
                    "CDC on MinIO: the pull flattens the prefix, and a CDC destination nests \
                     sub-prefixes whose `_SUCCESS`/manifest names collide"
                        .into(),
                );
            }
            Some(CloudDest::S3 { .. }) => {}
            Some(CloudDest::GcsLive { .. }) => {
                return Some(
                    "real GCS destination: not pulled by the default oracle (needs \
                     RIVET_TEST_GCS_BUCKET and ambient gcloud credentials)"
                        .into(),
                );
            }
            Some(_) => {
                return Some(
                    "fake-gcs / azurite destination: no whole-prefix pull exists yet".into(),
                );
            }
        }
        if self.tables.len() > 1 {
            return Some("multi-table capture: one sub-prefix per table, not graded yet".into());
        }
        if self
            .extra_lines
            .iter()
            .any(|l| l.starts_with("partition_by"))
        {
            return Some("partition_by: hive sub-prefixes, not graded yet".into());
        }
        if self.mode == "time_window" {
            return Some(
                "time_window: the window is relative to the run's clock and no manifest records it"
                    .into(),
            );
        }
        None
    }

    /// Whether each run delivers only what changed since the last (incremental, keyset-incremental, Mongo resume).
    fn oracle_is_delta(&self) -> bool {
        self.mode == "incremental"
            || self
                .extra_lines
                .iter()
                .any(|l| l.contains("keyset_incremental: true"))
            || self.source_lines.iter().any(|l| l.contains("resume: true"))
    }

    /// The directory the config (and so the state DB) lives in.
    fn config_dir(&self) -> PathBuf {
        self.config_dir_override
            .clone()
            .unwrap_or_else(|| self.dir.path().to_path_buf())
    }
}

/// The query rivet ran: the rig's YAML string decoded, `${k}` replaced by `--param k=v` from `argv`.
fn rendered_query(q: &str, argv: &[String]) -> String {
    let mut q: String =
        serde_yaml_ng::from_str(&format!("\"{q}\"")).unwrap_or_else(|_| q.to_string());
    for w in argv.windows(2).filter(|w| w[0] == "--param") {
        if let Some((k, v)) = w[1].split_once('=') {
            q = q.replace(&format!("${{{k}}}"), v);
        }
    }
    q
}

/// Run `dev/release_oracle/rig_oracle.py grade` (pinned by uv.lock) over `spec`; its JSON verdict.
fn run_rig_oracle(spec: &serde_json::Value, verb: &str) -> serde_json::Value {
    use std::io::Write as _;
    let mut child = std::process::Command::new("uv")
        .args([
            "run",
            "--frozen",
            "--quiet",
            "python",
            "-m",
            "dev.release_oracle.rig_oracle",
            verb,
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn `uv run` for the rig oracle — install uv (the oracle is pinned by uv.lock)");
    child
        .stdin
        .take()
        .expect("oracle stdin")
        .write_all(spec.to_string().as_bytes())
        .expect("write the oracle spec");
    let out = child.wait_with_output().expect("wait for the rig oracle");
    assert!(
        out.status.success(),
        "the rig oracle itself failed (a harness error, never a verdict):\n{}\nspec: {spec}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "rig oracle printed no JSON verdict ({e}):\n{}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

/// Append one verdict line (`RIVET-ORACLE-<VERDICT> <test> [<export>] — <detail>`) to stderr and `RIVET_ORACLE_LOG`.
fn oracle_log(verdict: &str, export: &str, detail: &str) {
    let who = std::thread::current()
        .name()
        .unwrap_or("<unnamed test>")
        .to_string();
    let line = format!("RIVET-ORACLE-{verdict} {who} [{export}] — {detail}");
    eprintln!("{line}");
    let path = std::env::var("RIVET_ORACLE_LOG").unwrap_or_else(|_| {
        format!(
            "{}/rivet-oracle.log",
            std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into())
        )
    });
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = f.write_all(format!("{}\n", line.replace('\n', " ")).as_bytes());
    }
}

/// Names of the Success manifests under `dir`, via the one declared-manifest resolver.
fn success_manifests(dir: &Path) -> BTreeSet<String> {
    super::super::parquet::declared_manifests(dir)
        .into_iter()
        .filter(|p| {
            std::fs::read_to_string(p)
                .ok()
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .is_some_and(|d| {
                    d.get("status")
                        .and_then(|s| s.as_str())
                        .is_none_or(|s| s.eq_ignore_ascii_case("success"))
                })
        })
        .filter_map(|p| p.file_name()?.to_str().map(str::to_string))
        .collect()
}

/// The source URL the oracle reads through: a toxiproxy front replaced by its upstream (a toxic left active cannot skew the read-back), the LogMiner user by the table owner.
fn oracle_source_url(url: &str) -> String {
    if url == super::super::env::ORACLE_CDC_URL {
        return super::super::env::ORACLE_URL.to_string();
    }
    [
        (":15432/", ":5432/"),
        (":13306/", ":3306/"),
        (":13307/", ":3307/"),
        (":27019/", ":27017/"),
    ]
    .iter()
    .fold(url.to_string(), |u, (from, to)| u.replace(from, to))
}

impl Drop for Rig {
    /// A known-defect marker whose rig never disagreed fails the test: the defect is fixed and the marker must go.
    fn drop(&mut self) {
        if let Some(why) = &self.oracle_xfail
            && !self.oracle_xfailed.get()
            && !std::thread::panicking()
        {
            panic!("oracle known defect now passes — remove the marker: {why}");
        }
    }
}
