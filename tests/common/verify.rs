//! VERIFY — the default independent oracle for live tests. Every `rivet run|load|compact
//! --config <path>` started through the shared runners (`run_rivet*` in runner.rs,
//! `run_rivet_ok`, and the `Rig`) that exits 0 is graded: the FACTS come from the config
//! file itself (source type and URL, each export's relation, mode, columns and destination,
//! the state DB beside the config or `RIVET_STATE_URL`, the Success manifests the run
//! wrote) and go to `dev/release_oracle/rig_oracle.py`, which owns the one DuckDB session
//! and every check. Opt out only with `.no_oracle("<reason>")` on a rig or the
//! `RIVET_TEST_NO_ORACLE=<reason>` env on a raw run (both counted by a shrink-only
//! ceiling); an export the oracle cannot reach logs `RIVET-ORACLE-SKIP`, never silence.
//! A `Command::new(RIVET_BIN)` built by hand is not graded (also a ceiling).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_yaml_ng::Value;

/// The env a raw run sets (to its reason) to opt out of the default oracle.
pub const NO_ORACLE_ENV: &str = "RIVET_TEST_NO_ORACLE";

/// Success manifest names in the destination and its `snapshot/` leg, taken before an invocation.
type ManifestSnapshot = [BTreeSet<String>; 2];

/// What a caller adds to the config's facts: a strict known-defect marker and a key for a keyless relation.
#[derive(Default)]
pub(crate) struct Opts<'a> {
    pub xfail: Option<&'a str>,
    pub key: Option<&'a str>,
}

/// One graded invocation: the parsed config and, for `run`, each export's manifests before it.
pub(crate) struct Case {
    verb: String,
    cfg: Value,
    config_dir: PathBuf,
    cwd: PathBuf,
    params: Vec<(String, String)>,
    exports: Vec<Value>,
    before: Vec<Result<ManifestSnapshot, String>>,
}

/// Start grading `argv` (run with working directory `cwd`): `None` unless it is `run|load|compact --config <path>` and not opted out.
pub(crate) fn begin(argv: &[String], envs: &[(&str, &str)], cwd: Option<&Path>) -> Option<Case> {
    let verb = argv.first()?.clone();
    if !matches!(verb.as_str(), "run" | "load" | "compact") {
        return None;
    }
    let flag = |long: &str, short: &str| -> Vec<String> {
        let mut out = Vec::new();
        for (i, a) in argv.iter().enumerate() {
            if a == long || a == short {
                out.extend(argv.get(i + 1).cloned());
            } else if let Some(v) = a.strip_prefix(&format!("{long}=")) {
                out.push(v.to_string());
            }
        }
        out
    };
    let cfg_path = PathBuf::from(flag("--config", "-c").pop()?);
    let cwd = cwd
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().expect("cwd"));
    let cfg_path = cwd.join(cfg_path);
    if let Some(why) = env_of(envs, NO_ORACLE_ENV) {
        assert!(!why.trim().is_empty(), "{NO_ORACLE_ENV} needs a reason");
        return None;
    }
    let text = std::fs::read_to_string(&cfg_path).ok()?;
    let cfg: Value = serde_yaml_ng::from_str(&text).ok()?;
    let only = flag("--export", "-e");
    let exports: Vec<Value> = cfg
        .get("exports")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter(|e| only.is_empty() || only.iter().any(|n| Some(n.as_str()) == s(e, "name")))
        .cloned()
        .collect();
    let params = flag("--param", "-p")
        .iter()
        .filter_map(|p| {
            p.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect();
    let mut case = Case {
        verb,
        config_dir: cfg_path.parent().map(Path::to_path_buf).unwrap_or_default(),
        cfg,
        cwd,
        params,
        exports,
        before: Vec::new(),
    };
    if case.verb == "run" {
        case.before = case
            .exports
            .iter()
            .map(|e| {
                case.unreachable(e)?;
                // A bucket the test has not created yet holds no manifest.
                let out =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| case.local_out(e)))
                        .map_err(|_| String::new());
                Ok(out.map(|o| manifests_of(&o)).unwrap_or_default())
            })
            .collect();
    }
    Some(case)
}

/// [`begin`] for a raw runner helper: only in the live suite (other test binaries drive rivet without a stand).
pub(crate) fn begin_raw(
    argv: &[String],
    envs: &[(&str, &str)],
    cwd: Option<&Path>,
) -> Option<Case> {
    module_path!()
        .starts_with("live_suite")
        .then(|| begin(argv, envs, cwd))
        .flatten()
}

/// Grade a finished invocation that exited 0; panics with every disagreement, returns whether a known defect disagreed as marked.
pub(crate) fn finish(case: Case, envs: &[(&str, &str)], opts: &Opts) -> bool {
    let mut xfailed = false;
    for (i, e) in case.exports.iter().enumerate() {
        let name = s(e, "name").unwrap_or("?").to_string();
        let verdict = if case.verb == "run" {
            case.grade_run(e, &case.before[i], envs, opts)
        } else {
            case.grade_load(e, envs, opts)
        };
        match verdict {
            Err(why) => log("SKIP", &name, &why),
            Ok((spec, verb)) => xfailed |= verdict_of(&name, &spec, verb, opts),
        }
    }
    xfailed
}

impl Case {
    /// The `run` spec for one export: its facts plus the manifests this run wrote.
    fn grade_run(
        &self,
        e: &Value,
        before: &Result<ManifestSnapshot, String>,
        envs: &[(&str, &str)],
        opts: &Opts,
    ) -> Result<(serde_json::Value, &'static str), String> {
        let [seen, seen_snap] = before.clone()?;
        let out = &self.local_out(e);
        let snap = &out.join("snapshot");
        let (now, now_snap) = (success_manifests(out), success_manifests(snap));
        if now.is_subset(&seen) && now_snap.is_subset(&seen_snap) {
            return Err("the run wrote no new Success manifest".into());
        }
        // CDC and delta modes grade the cumulative output; a snapshot run grades only what it declared.
        let cumulative = s(e, "mode") == Some("cdc") || self.is_delta(e);
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
        let mut spec = self.facts(e, envs, opts)?;
        spec.as_object_mut().expect("facts are an object").extend(
            serde_json::json!({
                "cumulative": cumulative,
                "out_dir": out,
                "manifests": graded(&now, &seen),
                "new_manifests": fresh(&now, &seen),
                "snapshot_dir": snap,
                "snapshot_manifests": graded(&now_snap, &seen_snap),
                "new_snapshot_manifests": fresh(&now_snap, &seen_snap),
            })
            .as_object()
            .expect("an object")
            .clone(),
        );
        Ok((spec, "grade"))
    }

    /// The `load`/`compact` spec for one export: its facts plus the config's `load:` block.
    fn grade_load(
        &self,
        e: &Value,
        envs: &[(&str, &str)],
        opts: &Opts,
    ) -> Result<(serde_json::Value, &'static str), String> {
        if self.is_backfill_recipe(e) {
            return Err(
                "load: a `cdc.backfill` recipe is a read recipe, never a load target".into(),
            );
        }
        if e.get("tables").is_some() {
            return Err(
                "load: a multi-table capture loads one table per source table, not graded yet"
                    .into(),
            );
        }
        let load = self
            .cfg
            .get("load")
            .filter(|l| l.is_mapping())
            .ok_or("load: no top-level `load:` block to read the target from")?;
        let password = s(load, "password_env")
            .and_then(|k| env_of(envs, k))
            .unwrap_or_default();
        let cdc = yaml_text(e.get("cdc"));
        let mut spec = self.facts(e, envs, opts)?;
        spec.as_object_mut().expect("facts are an object").extend(
            serde_json::json!({
                "load": serde_json::to_value(load).unwrap_or_default(),
                "password": password,
                "export": s(e, "name"),
                "snapshot": cdc.contains("initial: snapshot") || cdc.contains("backfill"),
            })
            .as_object()
            .expect("an object")
            .clone(),
        );
        Ok((spec, "grade-load"))
    }

    /// The source facts every oracle verb needs: engine, URL, relation, key, overrides, cursor, state DB.
    fn facts(
        &self,
        e: &Value,
        envs: &[(&str, &str)],
        opts: &Opts,
    ) -> Result<serde_json::Value, String> {
        let src = self.cfg.get("source").ok_or("no `source:` block")?;
        let raw = match (s(src, "url"), s(src, "url_env"), s(src, "url_file")) {
            (Some(u), _, _) => u.to_string(),
            (_, Some(k), _) => env_of(envs, k).ok_or(format!("source.url_env `{k}` is not set"))?,
            (_, _, Some(f)) => std::fs::read_to_string(self.cwd.join(f))
                .map_err(|err| format!("source.url_file `{f}`: {err}"))?
                .trim()
                .to_string(),
            _ => return Err("structured source (host/user/database): not graded yet".into()),
        };
        let url = source_url(&raw);
        let state = env_of(envs, "RIVET_STATE_URL")
            .filter(|u| u.starts_with("postgres"))
            .or_else(|| {
                let db = self.config_dir.join(".rivet_state.db");
                db.is_file().then(|| db.display().to_string())
            });
        let query = match (s(e, "query"), s(e, "query_file")) {
            (Some(q), _) => Some(q.to_string()),
            (_, Some(f)) => Some(
                std::fs::read_to_string(self.config_dir.join(f))
                    .map_err(|err| format!("query_file `{f}`: {err}"))?,
            ),
            _ => None,
        }
        .map(|mut q| {
            for (k, v) in &self.params {
                q = q.replace(&format!("${{{k}}}"), v);
            }
            q
        });
        let overrides: Vec<String> = e
            .get("columns")
            .and_then(Value::as_mapping)
            .map(|m| {
                m.keys()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let cursor_expr = (s(e, "incremental_cursor_mode") == Some("coalesce")).then(|| {
            format!(
                "coalesce(\"{}\", \"{}\")",
                s(e, "cursor_column").unwrap_or_default(),
                s(e, "cursor_fallback_column").unwrap_or_default()
            )
        });
        Ok(serde_json::json!({
            "engine": s(src, "type"),
            "export": s(e, "name"),
            "url": url,
            "database": url.rsplit('/').next().and_then(|d| d.split('?').next()).unwrap_or(""),
            "table": s(e, "table"),
            "query": query,
            "mode": if s(e, "mode") == Some("cdc") { "cdc" } else { "batch" },
            "key": opts.key.into_iter().collect::<Vec<_>>(),
            "overrides": overrides,
            "cursor_expr": cursor_expr,
            "state": state,
            "capture_instance": e.get("cdc").and_then(|c| s(c, "capture_instance")),
        }))
    }

    /// Why this export's output cannot be graded, or `Ok` when it can.
    fn unreachable(&self, e: &Value) -> Result<(), String> {
        let format = s(e, "format").unwrap_or("parquet");
        if format != "parquet" {
            return Err(format!("format `{format}`: the oracle grades parquet"));
        }
        let dest = e.get("destination").ok_or("no destination")?;
        match s(dest, "type") {
            Some("local") if s(dest, "path").is_some_and(|p| p.contains('{')) => {
                return Err("a placeholder destination path: not resolved yet".into());
            }
            Some("local") => {}
            Some("stdout") => return Err("stdout destination: nothing durable to read".into()),
            Some("s3") if s(e, "mode") == Some("cdc") => {
                return Err(
                    "CDC on MinIO: the pull flattens the prefix, and a CDC destination nests \
                            sub-prefixes whose `_SUCCESS`/manifest names collide"
                        .into(),
                );
            }
            Some("s3") if s(dest, "endpoint").is_some_and(|u| u.contains(":9000")) => {}
            Some("gcs") if s(dest, "endpoint").is_none() => {
                return Err(
                    "real GCS destination: not pulled by the default oracle (needs \
                            RIVET_TEST_GCS_BUCKET and ambient gcloud credentials)"
                        .into(),
                );
            }
            other => {
                return Err(format!(
                    "{} destination: no whole-prefix pull exists yet",
                    other.unwrap_or("?")
                ));
            }
        }
        if e.get("tables").is_some() {
            return Err("multi-table capture: one sub-prefix per table, not graded yet".into());
        }
        if e.get("partition_by").is_some() {
            return Err("partition_by: hive sub-prefixes, not graded yet".into());
        }
        if s(e, "mode") == Some("time_window") {
            return Err(
                "time_window: the window is relative to the run's clock and no manifest records it"
                    .into(),
            );
        }
        Ok(())
    }

    /// Whether a batch export is some CDC export's `backfill:` baseline (named, or paired by table), which `rivet load` never loads.
    fn is_backfill_recipe(&self, e: &Value) -> bool {
        let leaf = |t: &str| t.rsplit('.').next().unwrap_or(t).to_lowercase();
        let (Some(name), Some(table)) = (s(e, "name"), s(e, "table")) else {
            return false;
        };
        s(e, "mode") != Some("cdc")
            && self
                .cfg
                .get("exports")
                .and_then(Value::as_sequence)
                .into_iter()
                .flatten()
                .filter(|c| s(c, "mode") == Some("cdc"))
                .filter_map(|c| Some((c, c.get("cdc")?.get("backfill")?)))
                .any(|(c, spec)| {
                    let captured: Vec<String> = s(c, "table")
                        .into_iter()
                        .map(str::to_string)
                        .chain(
                            c.get("tables")
                                .and_then(Value::as_sequence)
                                .into_iter()
                                .flatten()
                                .filter_map(|t| t.as_str().map(str::to_string)),
                        )
                        .collect();
                    yaml_text(Some(spec)).contains(name)
                        || captured.iter().any(|t| leaf(t) == leaf(table))
                })
    }

    /// Whether each run delivers only what changed since the last (incremental, keyset-incremental, Mongo resume).
    fn is_delta(&self, e: &Value) -> bool {
        s(e, "mode") == Some("incremental")
            || e.get("keyset_incremental").and_then(Value::as_bool) == Some(true)
            || yaml_text(self.cfg.get("source")).contains("resume: true")
    }

    /// The destination as a local directory: the configured path, or a MinIO prefix pulled whole.
    fn local_out(&self, e: &Value) -> PathBuf {
        let dest = e.get("destination").expect("a destination");
        if s(dest, "type") == Some("s3") {
            let into = self.config_dir.join(super::unique_name("oracle_pull"));
            super::storage::minio_pull_prefix(
                s(dest, "bucket").expect("an s3 bucket"),
                s(dest, "prefix").unwrap_or(""),
                &into,
            );
            return into;
        }
        self.cwd
            .join(s(dest, "path").expect("a local destination path"))
    }
}

/// A string field of a YAML mapping.
fn s<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

/// A YAML node as text, for substring facts.
fn yaml_text(v: Option<&Value>) -> String {
    v.and_then(|v| serde_yaml_ng::to_string(v).ok())
        .unwrap_or_default()
}

/// `k` from the invocation's env, else the process env.
fn env_of(envs: &[(&str, &str)], k: &str) -> Option<String> {
    envs.iter()
        .find(|(n, _)| *n == k)
        .map(|(_, v)| v.to_string())
        .or_else(|| std::env::var(k).ok())
}

/// Both manifest legs of `out`.
fn manifests_of(out: &Path) -> ManifestSnapshot {
    [
        success_manifests(out),
        success_manifests(&out.join("snapshot")),
    ]
}

/// Run one oracle verb over `spec`; log PASS / SKIP / XFAIL, or panic with every disagreement. Returns whether it XFAILed.
fn verdict_of(name: &str, spec: &serde_json::Value, verb: &str, opts: &Opts) -> bool {
    let t0 = std::time::Instant::now();
    let verdict = run_rig_oracle(spec, verb);
    let took = format!("{verb} {} ms", t0.elapsed().as_millis());
    if let Some(why) = verdict["skip"].as_str() {
        log("SKIP", name, &format!("{took}: {why}"));
        return false;
    }
    let failures: Vec<String> = verdict["failures"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f.as_str().map(str::to_string))
        .collect();
    if failures.is_empty() {
        log("PASS", name, &format!("{took} {}", verdict["facts"]));
        return false;
    }
    if let Some(why) = opts.xfail {
        log("XFAIL", name, &format!("{why} — {}", failures.join(" | ")));
        return true;
    }
    log("FAIL", name, &failures.join(" | "));
    panic!(
        "rig oracle: export '{name}' disagrees with its source / rivet's own ledger \
         (dev/release_oracle/rig_oracle.py {verb}; opt out only with `.no_oracle(\"<why>\")` or \
         {NO_ORACLE_ENV}=<why>):\n  - {}\nspec: {spec}\nverdict: {verdict}",
        failures.join("\n  - ")
    );
}

/// Run `dev/release_oracle/rig_oracle.py <verb>` (pinned by uv.lock) over `spec`; its JSON verdict.
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
pub(crate) fn log(verdict: &str, export: &str, detail: &str) {
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
    super::parquet::declared_manifests(dir)
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
fn source_url(url: &str) -> String {
    if url == super::env::ORACLE_CDC_URL {
        return super::env::ORACLE_URL.to_string();
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
