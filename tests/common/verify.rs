//! VERIFY — the default independent oracle for live tests. Every `rivet run|load|compact
//! --config <path>`, `rivet apply <config.yaml>` and `rivet cdc` (graded as the `run` of the config
//! its flags are equivalent to; a `--max-events` run that reached its cap defers what it owed past
//! it to the stream's next run, logged `RIVET-ORACLE-DEFERRED`; the census requires that run's verdict) started through the shared runners
//! (`run_rivet*` in runner.rs, `run_rivet_ok`, and the `Rig`) in the live suite,
//! live_type_golden or live_differential is graded by how it exits ([`settle`]). One that exits 0
//! is graded against its source; one that does not is graded against the snapshot taken before it
//! (refusal.rs: destination trees, checkpoint files, state rows; `RIVET-ORACLE-REFUSED`, or
//! `RIVET-ORACLE-UNGRADED` for a run the test crashed itself). For exit 0 the FACTS come from the
//! config file itself (source type and URL, each export's relation, mode, columns and
//! destination, the state DB beside the config or `RIVET_STATE_URL`, the Success manifests
//! the run wrote) and go to `dev/release_oracle/rig_oracle.py`, which owns the one DuckDB
//! session and every check. A CDC export without a snapshot leg is graded against source
//! images the oracle takes before each run (every row changed between the stream's previous
//! successful run, else its anchor, and this one must be in it). A stream is its PostgreSQL
//! slot or its checkpoint file, whichever config names it; its anchor is recorded before a
//! run, including one that then fails, or by `Rig::pin_binlog_here` where a test writes the
//! checkpoint itself. A stream anchored before any run the oracle saw is
//! `RIVET-ORACLE-PARTIAL` on its first run, never a plain PASS. Opt out only with `.no_oracle("<reason>")` on
//! a rig or the `RIVET_TEST_NO_ORACLE=<reason>` env on a raw run (both counted by a
//! shrink-only ceiling, both logged `RIVET-ORACLE-OFF`); a run's one `destination: stdout` export
//! is graded from the bytes its runner captured (`Case::delivered`); an export the oracle cannot
//! reach logs `RIVET-ORACLE-SKIP`; an exception inside the oracle FAILs the test as an oracle
//! error. A `Rig::spawn_args_env` child is graded when its caller reaps it;
//! a hand-built `Command::new(RIVET_BIN)` is not graded (under its own ceiling).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_yaml_ng::Value;

use super::refusal::{self, Leftover};

/// The env a raw run sets (to its reason) to opt out of the default oracle.
pub const NO_ORACLE_ENV: &str = "RIVET_TEST_NO_ORACLE";

/// Success manifest names in the destination and its `snapshot/` leg, taken before an invocation.
type ManifestSnapshot = [BTreeSet<String>; 2];

/// A known product defect a rig declares: the one export and failure class it excuses (rig_oracle.KNOWN_DEFECT_CLASSES).
#[derive(Clone, Copy)]
pub(crate) struct KnownDefect<'a> {
    pub export: &'a str,
    pub class: &'a str,
    pub reason: &'a str,
}

/// What a caller adds to the config's facts: a strict known-defect marker and a key for a keyless relation.
#[derive(Default)]
pub(crate) struct Opts<'a> {
    pub xfail: Option<KnownDefect<'a>>,
    pub key: Option<&'a str>,
    /// What the caller declared a failed run may leave (refusal.rs).
    pub leaves: &'a [Leftover],
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
    /// A `--resume` run completes a crashed run's plan, made before this invocation.
    resume: bool,
    /// The chunk ranges a sealed plan replays (computed when it was planned), else `null`.
    replay: serde_json::Value,
    /// A `rivet cdc` invocation's own surface, graded as the `run` of the config it is equivalent to.
    cli: Option<CdcCli>,
    /// Per export, the NDJSON change lines this `rivet cdc` run printed for its table.
    events: Vec<usize>,
    /// What the invocation's destinations, checkpoints and state held before it started.
    snapshot: Option<refusal::Snapshot>,
    /// When the invocation began: its `export_metrics` rows are the ones recorded since.
    began: String,
    /// The file holding what the run printed for its one `destination: stdout` export.
    stdout: Option<PathBuf>,
}

/// What a `rivet cdc` invocation adds to its equivalent config: its `--max-events` cap, and whether it printed NDJSON (no `--output`).
#[derive(Clone, Copy)]
struct CdcCli {
    max_events: Option<usize>,
    ndjson: bool,
}

/// The config a `rivet cdc` invocation is equivalent to (src/cli/dispatch.rs `dispatch_cdc`): one export per `--table`, its stream named by `--slot` (PostgreSQL) or `--checkpoint`; a run with no checkpoint is a stream of its own, anchored at its open.
fn cdc_cli_config(
    flag: &dyn Fn(&str, &str) -> Vec<String>,
    envs: &[(&str, &str)],
    cwd: &Path,
) -> Result<(Value, CdcCli), String> {
    let one = |f: &str| flag(f, f).pop();
    let (field, raw) = [
        ("url", "--source"),
        ("url_env", "--source-env"),
        ("url_file", "--source-file"),
    ]
    .into_iter()
    .find_map(|(k, f)| one(f).map(|v| (k, v)))
    .ok_or("`rivet cdc` with no source flag")?;
    let url = match field {
        "url" => raw.clone(),
        "url_env" => env_of(envs, &raw).ok_or(format!("--source-env `{raw}` is not set"))?,
        _ => std::fs::read_to_string(cwd.join(&raw))
            .map_err(|err| format!("--source-file `{raw}`: {err}"))?
            .trim()
            .to_string(),
    };
    let engine = match url.split("://").next().unwrap_or_default() {
        "postgres" | "postgresql" => "postgres",
        "mysql" => "mysql",
        "sqlserver" | "mssql" => "mssql",
        "mongodb" | "mongodb+srv" => "mongo",
        "oracle" => "oracle",
        other => return Err(format!("`rivet cdc` over an unknown scheme `{other}://`")),
    };
    let tables = flag("--table", "--table");
    if tables.is_empty() {
        return Err(
            "`rivet cdc` with no --table captures every table: nothing names the relation to grade"
                .into(),
        );
    }
    let output = one("--output");
    let cli = CdcCli {
        max_events: one("--max-events").and_then(|n| n.parse().ok()),
        ndjson: output.is_none(),
    };
    let images = image_dir();
    let checkpoint = one("--checkpoint").unwrap_or_else(|| {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let fresh = format!("no-checkpoint-{}-{n}-{nanos}", std::process::id());
        images.join(fresh).display().to_string()
    });
    let format = match &output {
        Some(_) => one("--format").unwrap_or_else(|| "parquet".into()),
        None => "ndjson".into(),
    };
    let dest = output.unwrap_or_else(|| images.join("ndjson-out").display().to_string());
    let exports: Vec<serde_json::Value> = tables
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t,
                "mode": "cdc",
                "table": t,
                "format": format,
                "cdc": {
                    "slot": one("--slot").unwrap_or_else(|| "rivet_slot".into()),
                    "checkpoint": checkpoint,
                    "capture_instance": one("--capture-instance"),
                },
                "destination": {"type": "local", "path": dest},
            })
        })
        .collect();
    let cfg = serde_json::json!({"source": {"type": engine, field: raw}, "exports": exports});
    let cfg = serde_yaml_ng::to_value(cfg).map_err(|e| e.to_string())?;
    Ok((cfg, cli))
}

/// The directory the oracle keeps its source images and stream records in.
fn image_dir() -> PathBuf {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(target)
        .join("rivet-oracle-images");
    std::fs::create_dir_all(&dir).expect("create the oracle image dir");
    dir
}

/// Start grading `argv` (run with working directory `cwd`): `None` unless it is `run|load|compact --config <path>`, `apply <config.yaml>` or `cdc` and not opted out.
pub(crate) fn begin(argv: &[String], envs: &[(&str, &str)], cwd: Option<&Path>) -> Option<Case> {
    let mut case = parse(argv, envs, cwd)?;
    let scope = case.scope(envs);
    if case.verb == "run" {
        case.before = case
            .exports
            .iter()
            .zip(&scope.exports)
            .map(|(e, (_, root))| {
                case.unreachable(e)?;
                if writes_stdout(e) {
                    return Ok(ManifestSnapshot::default());
                }
                let out = root.clone()?;
                if case.needs_image(e) {
                    // A run that later fails still delivered what its Success manifests declare.
                    case.stream_dirs(e, &out);
                }
                Ok(case.manifests_of(e, &out))
            })
            .collect();
        // Every row changed before a CDC run opens its stream must be in it: image the source first.
        for e in case.exports.iter().filter(|e| case.needs_image(e)) {
            case.take_image(e, envs, &case.image(e, "begin"));
        }
    }
    case.snapshot = Some(refusal::Snapshot::take(&scope));
    Some(case)
}

/// Grade a finished invocation by how it exited: exit 0 against its source ([`finish`]), anything else against the snapshot taken before it. Returns whether a known defect disagreed as marked.
pub(crate) fn settle(
    mut case: Case,
    status: std::process::ExitStatus,
    stdout: &[u8],
    envs: &[(&str, &str)],
    opts: &Opts,
) -> bool {
    if status.success() {
        case.delivered(stdout);
        return finish(case, envs, opts);
    }
    case.refused(status, envs, opts)
}

/// Start every CDC stream `cfg` names afresh at the source as it stands now, recording the anchor a run that opened its stream here would; its checkpoint must not exist yet.
pub(crate) fn anchor_streams_here(cfg: &Path) {
    let argv = ["run", "--config", &cfg.display().to_string()].map(String::from);
    let case = parse(&argv, &[], None).expect("the rig's own config parses");
    let mut anchored = 0;
    for e in case.exports.iter().filter(|e| case.needs_image(e)) {
        for f in case.stream_records(e) {
            let _ = std::fs::remove_file(f);
        }
        let got = case.take_image(e, &[], &case.image(e, "begin"));
        assert_eq!(
            got.as_ref().map(|v| v["anchor"].clone()),
            Some("begin".into()),
            "the oracle did not record {cfg:?}'s anchor at this position: {got:?}"
        );
        anchored += 1;
    }
    assert!(
        anchored > 0,
        "{cfg:?} names no single-table CDC stream to anchor"
    );
}

/// Move the checkpoint `from` -> `to` and the oracle's record of its stream with it, so `cfg` run from `cwd` continues one stream across the move.
pub(crate) fn move_checkpoint(cfg: &Path, cwd: &Path, from: &Path, to: &Path) {
    let argv = ["run", "--config", &cfg.display().to_string()].map(String::from);
    let case = parse(&argv, &[], Some(cwd)).expect("the rig's own config parses");
    let streams: Vec<&Value> = case
        .exports
        .iter()
        .filter(|e| case.needs_image(e))
        .collect();
    let before: Vec<Vec<PathBuf>> = streams.iter().map(|e| case.stream_records(e)).collect();
    std::fs::rename(from, to).expect("move the checkpoint");
    for (e, old) in streams.iter().zip(before) {
        let new = case.stream_records(e);
        assert_ne!(
            old, new,
            "the move left {cfg:?}'s stream where it was: nothing to follow"
        );
        for (o, n) in old.iter().zip(new).filter(|(o, _)| o.exists()) {
            std::fs::rename(o, n).expect("move the stream's record");
        }
    }
}

/// The invocation `argv` describes, parsed from its config; no source is read.
fn parse(argv: &[String], envs: &[(&str, &str)], cwd: Option<&Path>) -> Option<Case> {
    let mut verb = argv.first()?.clone();
    if !matches!(verb.as_str(), "run" | "load" | "compact" | "apply" | "cdc") {
        return None;
    }
    let cdc_cli = verb == "cdc";
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
    let cwd = cwd
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().expect("cwd"));
    // A sealed plan artifact names its export and the config it was planned from; its resolved destination and query win.
    let mut sealed: Option<(String, Value, String)> = None;
    let mut replay = serde_json::Value::Null;
    let cfg_path = if cdc_cli {
        // `rivet cdc` anchors its relative paths to its working directory (src/cli/dispatch.rs `dispatch_cdc`).
        verb = "run".into();
        PathBuf::from("rivet-cdc.yaml")
    } else if verb == "apply" {
        // `apply <config.yaml>` runs the config's exports wave by wave: graded as a `run`.
        let plan = argv.get(1).filter(|p| !p.starts_with('-'))?;
        verb = "run".into();
        if plan.ends_with(".yaml") || plan.ends_with(".yml") {
            PathBuf::from(plan)
        } else {
            let doc: serde_json::Value = std::fs::read_to_string(cwd.join(plan))
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())?;
            let (Some(cfg), Some(name)) =
                (doc["config_path"].as_str(), doc["export_name"].as_str())
            else {
                log(
                    "SKIP",
                    "*",
                    "apply of a plan artifact that names no config_path or export_name",
                );
                return None;
            };
            let plan = &doc["resolved_plan"];
            let dest = serde_yaml_ng::to_value(&plan["destination"]).ok()?;
            let query = plan["base_query"].as_str().unwrap_or_default().to_string();
            replay = doc["computed"]["chunk_ranges"].clone();
            sealed = Some((name.to_string(), dest, query));
            PathBuf::from(cfg)
        }
    } else {
        PathBuf::from(flag("--config", "-c").pop()?)
    };
    let cfg_path = cwd.join(cfg_path);
    if let Some(why) = env_of(envs, NO_ORACLE_ENV) {
        assert!(!why.trim().is_empty(), "{NO_ORACLE_ENV} needs a reason");
        log("OFF", "*", &why);
        return None;
    }
    let params: Vec<(String, String)> = flag("--param", "-p")
        .iter()
        .filter_map(|p| {
            p.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect();
    let mut cli = None;
    let cfg = if cdc_cli {
        match cdc_cli_config(&flag, envs, &cwd) {
            Ok((cfg, c)) => {
                cli = Some(c);
                cfg
            }
            Err(why) => {
                log("SKIP", "*", &why);
                return None;
            }
        }
    } else {
        let Some(text) = std::fs::read_to_string(&cfg_path).ok() else {
            log(
                "SKIP",
                "*",
                &format!("config {} is unreadable", cfg_path.display()),
            );
            return None;
        };
        let Some(cfg) = serde_yaml_ng::from_str::<Value>(&resolve_vars(&text, &params, envs)).ok()
        else {
            log(
                "SKIP",
                "*",
                &format!("config {} does not parse", cfg_path.display()),
            );
            return None;
        };
        cfg
    };
    let only = match &sealed {
        Some((name, ..)) => vec![name.clone()],
        None => flag("--export", "-e"),
    };
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let exports: Vec<Value> = cfg
        .get("exports")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter(|e| only.is_empty() || only.iter().any(|n| Some(n.as_str()) == s(e, "name")))
        .flat_map(|e| {
            let mut e = e.clone();
            if let Some((_, dest, query)) = &sealed {
                e["destination"] = dest.clone();
                if let Some(m) = e
                    .as_mapping_mut()
                    .filter(|m| m.contains_key("query") || m.contains_key("query_file"))
                {
                    m.remove("query_file");
                    m.insert("query".into(), query.as_str().into());
                }
            }
            resolve_placeholders(&mut e, &date);
            if verb == "run" {
                per_table(&e)
            } else {
                vec![e]
            }
        })
        .collect();
    Some(Case {
        verb,
        config_dir: cfg_path.parent().map(Path::to_path_buf).unwrap_or_default(),
        cfg,
        cwd,
        params,
        exports,
        before: Vec::new(),
        resume: argv.iter().any(|a| a == "--resume"),
        replay,
        cli,
        events: Vec::new(),
        snapshot: None,
        began: chrono::Utc::now().to_rfc3339(),
        stdout: None,
    })
}

/// [`begin`] for a raw runner helper: only in the binaries that run against the stand (offline test binaries drive rivet without one).
pub(crate) fn begin_raw(
    argv: &[String],
    envs: &[(&str, &str)],
    cwd: Option<&Path>,
) -> Option<Case> {
    ["live_suite", "live_type_golden", "live_differential"]
        .iter()
        .any(|b| module_path!().starts_with(b))
        .then(|| begin(argv, envs, cwd))
        .flatten()
}

/// Grade a finished invocation that exited 0; panics with every disagreement, returns whether a known defect disagreed as marked.
fn finish(case: Case, envs: &[(&str, &str)], opts: &Opts) -> bool {
    let mut xfailed = false;
    assert!(
        !case.cli.is_some_and(|c| c.ndjson) || case.events.len() == case.exports.len(),
        "a `rivet cdc` NDJSON run reached the oracle without its stdout: its runner must hand it to `Case::delivered`"
    );
    let deferred = case.deferred();
    for (i, e) in case.exports.iter().enumerate() {
        let name = s(e, "name").unwrap_or("?").to_string();
        if let Some(cap) = deferred {
            log(
                "DEFERRED",
                &name,
                &format!(
                    "a bounded run reached --max-events {cap}: what it owed past its bound is graded on the stream's next run, against everything since this run's base"
                ),
            );
            continue;
        }
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
    if case.verb == "run" && deferred.is_none() {
        // This run's pre-run image becomes the stream's `prev` (and, on its first successful run, its `anchor`).
        for e in case.exports.iter().filter(|e| case.needs_image(e)) {
            let [begin, prev, anchor] = ["begin", "prev", "anchor"].map(|k| case.image(e, k));
            let first = !image_exists(&anchor);
            for ext in ["parquet", "parquet.missing"] {
                let _ = std::fs::remove_file(with_ext(&prev, ext));
                let from = with_ext(&begin, ext);
                if from.exists() {
                    if first {
                        std::fs::copy(&from, with_ext(&anchor, ext))
                            .expect("keep the anchor image");
                    }
                    std::fs::rename(&from, with_ext(&prev, ext)).expect("keep the prev image");
                }
            }
        }
    }
    xfailed
}

/// Whether an image (or its absent-table marker) exists at `base`.
fn image_exists(base: &Path) -> bool {
    with_ext(base, "parquet").exists() || with_ext(base, "parquet.missing").exists()
}

impl Case {
    /// Where this invocation keeps its state: the run's `RIVET_STATE_URL` or the backend under test when Postgres, else SQLite beside the config; `rivet cdc` keeps none (src/cli/dispatch.rs).
    fn state_at(&self, envs: &[(&str, &str)]) -> Option<refusal::StateAt> {
        if self.cli.is_some() {
            return None;
        }
        Some(
            envs.iter()
                .find(|(n, _)| *n == "RIVET_STATE_URL")
                .map(|(_, v)| v.to_string())
                .or_else(super::state::state_url_under_test)
                .filter(|u| u.starts_with("postgres"))
                .map_or_else(
                    || refusal::StateAt::Sqlite(self.config_dir.join(".rivet_state.db")),
                    refusal::StateAt::Postgres,
                ),
        )
    }

    /// What the refusal grade fingerprints: each export's destination tree (a cloud prefix only around a `run` the oracle can pull), the checkpoint files, the state.
    fn scope(&self, envs: &[(&str, &str)]) -> refusal::Scope {
        let root = |e: &Value| -> Result<PathBuf, String> {
            if e.get("destination").and_then(|d| s(d, "type")) != Some("local") {
                if self.verb != "run" {
                    return Err("a cloud prefix is not pulled around a load or a compact".into());
                }
                self.unreachable(e)?;
            }
            self.local_out(e)
        };
        let name = |e: &Value| {
            let whole = e.get("__stream").unwrap_or(e);
            s(whole, "name").unwrap_or("?").to_string()
        };
        let mut checkpoints: Vec<PathBuf> = self
            .exports
            .iter()
            .filter_map(|e| self.checkpoint(e))
            .collect();
        checkpoints.sort();
        checkpoints.dedup();
        refusal::Scope {
            exports: self.exports.iter().map(|e| (name(e), root(e))).collect(),
            checkpoints,
            state: self.state_at(envs),
        }
    }

    /// Log that this invocation ended in a way no grade covers.
    pub(crate) fn ungraded(&self, why: &str) {
        log("UNGRADED", &self.label(), why);
    }

    /// The verdict line's export: the one export, else `*`.
    fn label(&self) -> String {
        match self.exports.as_slice() {
            [e] => s(e, "name").unwrap_or("?").to_string(),
            _ => "*".into(),
        }
    }

    /// Grade an invocation that did not exit 0 against its pre-run snapshot; panics on anything left behind undeclared, returns whether a known defect showed as marked.
    fn refused(self, status: std::process::ExitStatus, envs: &[(&str, &str)], opts: &Opts) -> bool {
        let exit = status
            .code()
            .map_or_else(|| "no exit code".to_string(), |c| format!("exit {c}"));
        if let Some(why) = refusal::crashed_by_the_test(status, envs) {
            self.ungraded(&format!(
                "{exit}: {why}; a crash is graded by the run that resumes it"
            ));
            return false;
        }
        let scope = self.scope(envs);
        let before = self.snapshot.as_ref().expect("begin took the snapshot");
        let (found, blind) = refusal::diff(&scope, before, &refusal::Snapshot::take(&scope));
        let states: BTreeSet<&String> = scope.exports.iter().map(|(n, _)| n).collect();
        let (found, delivered) = refusal::without_delivering_siblings(found, states.len());
        for e in &delivered {
            log(
                "UNGRADED",
                e,
                &format!(
                    "{exit}: this export recorded success beside a sibling that failed; its delivery was not graded"
                ),
            );
        }
        let mut declared = opts.leaves.to_vec();
        if let Some(raw) = envs
            .iter()
            .find(|(k, _)| *k == refusal::FAILED_RUN_LEAVES_ENV)
        {
            declared.extend(refusal::declared_in_env(raw.1));
        }
        let marker = opts
            .xfail
            .and_then(|k| Leftover::of_known_defect_class(k.class).map(|kind| (kind, k.reason)));
        let gaps = if blind.is_empty() {
            String::new()
        } else {
            format!("; {}", blind.join("; "))
        };
        let name = self.label();
        match refusal::judge(&found, &declared, marker) {
            refusal::Verdict::Refused {
                left,
                known,
                marked,
            } => {
                log("REFUSED", &name, &format!("{exit}: {left}{gaps}"));
                for (why, detail) in known {
                    log("XFAIL", &name, &format!("{why} — {detail}"));
                }
                marked
            }
            refusal::Verdict::Fail(lines) => {
                log("FAIL", &name, &format!("{exit}: {}", lines.join(" | ")));
                panic!(
                    "rig oracle: the invocation ended with {exit} and left behind what a failed run may not \
                     (tests/common/refusal.rs; declare a legitimate leftover with `.a_failed_run_may_leave(..)` or \
                     {}=<kinds>, a product defect with `Rig::oracle_known_defect(\"a failed run left: <kind>\", ..)`):\n  - {}{gaps}",
                    refusal::FAILED_RUN_LEAVES_ENV,
                    lines.join("\n  - ")
                );
            }
        }
    }

    /// Record what the run printed: the bytes of its one `destination: stdout` export, or a `rivet cdc` NDJSON run's change lines appended per export to its stream's event log.
    pub(crate) fn delivered(&mut self, stdout: &[u8]) {
        if let [e] = self.stdout_exports()[..] {
            let path = with_ext(
                &self.image(e, "stdout"),
                s(e, "format").unwrap_or("parquet"),
            );
            std::fs::write(&path, stdout).expect("keep the run's stdout");
            self.stdout = Some(path);
        }
        if !self.cli.is_some_and(|c| c.ndjson) {
            return;
        }
        let text = String::from_utf8_lossy(stdout);
        self.events = self
            .exports
            .iter()
            .map(|e| {
                let table = s(e, "table").unwrap_or_default();
                let leaf = table.rsplit('.').next().unwrap_or(table).to_lowercase();
                let mine: String = text
                    .lines()
                    .filter(|l| {
                        serde_json::from_str::<serde_json::Value>(l)
                            .ok()
                            .and_then(|v| v["table"].as_str().map(str::to_lowercase))
                            == Some(leaf.clone())
                    })
                    .map(|l| format!("{l}\n"))
                    .collect();
                use std::io::Write as _;
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(with_ext(&self.image(e, "events"), "jsonl"))
                    .and_then(|mut f| f.write_all(mine.as_bytes()))
                    .expect("append the stream's NDJSON events");
                mine.lines().count()
            })
            .collect();
    }

    /// `Some(cap)` when a `--max-events` run delivered its cap: it stopped at its bound, not at the log end it opened at.
    fn deferred(&self) -> Option<usize> {
        let cli = self.cli?;
        let cap = cli.max_events?;
        let got: usize = if cli.ndjson {
            self.events.iter().sum()
        } else {
            self.exports
                .iter()
                .zip(&self.before)
                .filter_map(|(e, before)| {
                    let out = self.local_out(e).ok()?;
                    let [seen, _] = before.as_ref().ok()?;
                    let [now, _] = self.manifests_of(e, &out);
                    Some(
                        now.difference(seen)
                            .filter_map(|m| std::fs::read_to_string(out.join(m)).ok())
                            .filter_map(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                            .filter_map(|d| d["row_count"].as_u64())
                            .sum::<u64>() as usize,
                    )
                })
                .sum()
        };
        (got >= cap).then_some(cap)
    }

    /// The `run` spec for one export: its facts plus the manifests this run wrote.
    fn grade_run(
        &self,
        e: &Value,
        before: &Result<ManifestSnapshot, String>,
        envs: &[(&str, &str)],
        opts: &Opts,
    ) -> Result<(serde_json::Value, &'static str), String> {
        let [seen, seen_snap] = before.clone()?;
        if writes_stdout(e) {
            let printed = self.stdout.as_ref().ok_or(
                "stdout destination: this runner did not hand the run's stdout to the oracle",
            )?;
            let mut spec = self.facts(e, envs, opts)?;
            spec["format"] = s(e, "format").unwrap_or("parquet").into();
            spec["stdout"] = printed.display().to_string().into();
            spec["since"] = self.began.as_str().into();
            return Ok((spec, "grade-stdout"));
        }
        let out = &self.local_out(e)?;
        let snap = &out.join("snapshot");
        let [now, now_snap] = self.manifests_of(e, out);
        // No new manifest: a snapshot run must have had an empty source (the oracle asks it); CDC and delta are still graded cumulatively.
        let nothing_new = now.is_subset(&seen) && now_snap.is_subset(&seen_snap);
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
        if nothing_new && !cumulative && self.is_backfill_recipe(e) {
            return Err(
                "a `cdc.backfill` recipe feeds its CDC export's stream, not its own destination"
                    .into(),
            );
        }
        let mut spec = self.facts(e, envs, opts)?;
        spec.as_object_mut().expect("facts are an object").extend(
            serde_json::json!({
                "cumulative": cumulative,
                "nothing_new": nothing_new,
                "stream_dirs": if self.needs_image(e) { self.stream_dirs(e, out) } else { Vec::new() },
                "out_dir": out,
                "manifests": graded(&now, &seen),
                "new_manifests": fresh(&now, &seen),
                "snapshot_dir": snap,
                "snapshot_manifests": graded(&now_snap, &seen_snap),
                "new_snapshot_manifests": fresh(&now_snap, &seen_snap),
                "snapshot": self.declares_snapshot(e),
                "resume": self.resume,
                "replay": self.replay.as_array().is_some_and(|r| !r.is_empty()),
                "ranges": self.replay,
                "range_column": e.get("chunk_by_days").is_none().then(|| s(e, "chunk_column")).flatten(),
                "settle": e.get("settle").is_some(),
                "consumed": ([e.get("load"), self.cfg.get("load")]
                    .into_iter()
                    .flatten()
                    .find_map(|l| l.get("cleanup_source").and_then(Value::as_bool))
                    == Some(true)),
                "format": s(e, "format").unwrap_or("parquet"),
                "stream": self.stream(e)?,
                "partition_by": s(e, "partition_by"),
                "base": with_ext(&self.image(e, "prev"), "parquet"),
                "upper": with_ext(&self.image(e, "begin"), "parquet"),
                "anchor": with_ext(&self.image(e, "anchor"), "parquet"),
                "anchor_keys": with_ext(&self.image(e, "anchor-keys"), "parquet"),
                "cursor_record": with_ext(&self.image(e, "cursor"), "json"),
                "ndjson": with_ext(&self.image(e, "events"), "jsonl"),
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
        let mut spec = self.facts(e, envs, opts)?;
        spec.as_object_mut().expect("facts are an object").extend(
            serde_json::json!({
                "load": serde_json::to_value(load).unwrap_or_default(),
                "password": password,
                "export": s(e, "name"),
                "verb": self.verb,
                "delta": self.is_delta(e),
                "snapshot": self.declares_snapshot(e) || yaml_text(e.get("cdc")).contains("backfill"),
                "base": with_ext(&self.image(e, "anchor"), "parquet"),
                "upper": with_ext(&self.image(e, "prev"), "parquet"),
                "anchor_keys": with_ext(&self.image(e, "anchor-keys"), "parquet"),
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
        let db = self.config_dir.join(".rivet_state.db");
        let state = match self.state_at(envs) {
            Some(refusal::StateAt::Postgres(url)) => Some(url),
            Some(refusal::StateAt::Sqlite(db)) if db.is_file() => Some(db.display().to_string()),
            _ => None,
        };
        // A run with no findable state is graded PARTIAL, never a silent PASS of a ledger leg that was not compared.
        let state_missing = (state.is_none() && self.cli.is_none()).then(|| {
            format!(
                "no state DB: {} is absent and neither the run's RIVET_STATE_URL nor the backend under test (RIVET_GATE_STATE_URL) is postgres, so rivet's ledger (export_metrics, file_log) was not compared",
                db.display()
            )
        });
        let query = match (s(e, "query"), s(e, "query_file")) {
            (Some(q), _) => Some(q.to_string()),
            (_, Some(f)) => Some(
                std::fs::read_to_string(self.config_dir.join(f))
                    .map_err(|err| format!("query_file `{f}`: {err}"))?,
            ),
            _ => None,
        }
        .map(|q| resolve_vars(&q, &self.params, envs));
        let overrides: serde_json::Map<String, serde_json::Value> = e
            .get("columns")
            .and_then(Value::as_mapping)
            .into_iter()
            .flatten()
            .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str().into())))
            .collect();
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
            "state_missing": state_missing,
            "capture_instance": e.get("cdc").and_then(|c| s(c, "capture_instance")),
        }))
    }

    /// Why this export's output cannot be graded, or `Ok` when it can.
    fn unreachable(&self, e: &Value) -> Result<(), String> {
        let format = s(e, "format").unwrap_or("parquet");
        if !matches!(format, "parquet" | "csv") && !self.cli.is_some_and(|c| c.ndjson) {
            return Err(format!(
                "format `{format}`: the oracle grades parquet and csv"
            ));
        }
        let dest = e.get("destination").ok_or("no destination")?;
        match s(dest, "type") {
            Some("local") | Some("gcs") | Some("azure") => {}
            Some("stdout") if self.stdout_exports().len() == 1 => {}
            Some("stdout") => {
                return Err(
                    "stdout destination: several exports share one stdout, whose bytes name no export"
                        .into(),
                );
            }
            Some("s3") if s(dest, "endpoint").is_some_and(|u| u.contains(":9000")) => {}
            other => {
                return Err(format!(
                    "{} destination: no whole-prefix pull exists yet",
                    other.unwrap_or("?")
                ));
            }
        }
        if s(e, "mode") == Some("time_window") {
            return Err(
                "time_window: the window is relative to the run's clock and no manifest records it"
                    .into(),
            );
        }
        Ok(())
    }

    /// The exports of this invocation that write to stdout.
    fn stdout_exports(&self) -> Vec<&Value> {
        self.exports.iter().filter(|e| writes_stdout(e)).collect()
    }

    /// For one table of a multi-table capture: its table and every table's local destination (the run's ledger counts the whole stream).
    fn stream(&self, e: &Value) -> Result<serde_json::Value, String> {
        let Some(whole) = e.get("__stream") else {
            return Ok(serde_json::Value::Null);
        };
        let dirs = per_table(whole)
            .iter()
            .map(|t| self.local_out(t))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({"table": s(e, "table"), "dirs": dirs}))
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

    /// Whether a CDC export declares `initial: snapshot` (its stream carries a baseline leg).
    fn declares_snapshot(&self, e: &Value) -> bool {
        e.get("cdc").and_then(|c| s(c, "initial")) == Some("snapshot")
    }

    /// Whether a CDC export is graded from source images: one captured table.
    fn needs_image(&self, e: &Value) -> bool {
        s(e, "mode") == Some("cdc") && s(e, "table").is_some()
    }

    /// Where this stream's file of `kind` lives, without its extension (images: `begin` of this run, `prev` of the last successful run, `anchor` where the stream began; `cursor`: the delta record).
    fn image(&self, e: &Value, kind: &str) -> PathBuf {
        use std::hash::{Hash as _, Hasher as _};
        let mut h = std::hash::DefaultHasher::new();
        self.stream_id(e).hash(&mut h);
        image_dir().join(format!("{:016x}-{kind}", h.finish()))
    }

    /// Every file the oracle keeps for this export's stream, existing or not.
    fn stream_records(&self, e: &Value) -> Vec<PathBuf> {
        let kinds = [
            "begin",
            "prev",
            "anchor",
            "anchor-keys",
            "dests",
            "cursor",
            "events",
        ];
        let exts = ["parquet", "parquet.missing", "txt", "json", "jsonl"];
        kinds
            .iter()
            .flat_map(|k| exts.map(|x| with_ext(&self.image(e, k), x)))
            .collect()
    }

    /// The stream an export continues: a CDC table's PostgreSQL slot or checkpoint file (every config naming it shares it), else its config directory and name.
    fn stream_id(&self, e: &Value) -> String {
        let cdc = e.get("cdc").filter(|_| s(e, "mode") == Some("cdc"));
        let table = s(e, "table").unwrap_or_default();
        let pg = self.cfg.get("source").and_then(|src| s(src, "type")) == Some("postgres");
        match (
            cdc.and_then(|c| s(c, "slot")).filter(|_| pg),
            self.checkpoint(e),
        ) {
            (Some(slot), _) => format!("slot {slot} {table}"),
            (None, Some(ckpt)) if !pg => format!("checkpoint {} {table}", ckpt.display()),
            _ => format!(
                "{} {}",
                self.config_dir.display(),
                s(e, "name").unwrap_or("?")
            ),
        }
    }

    /// The CDC checkpoint file the export names, resolved as rivet resolves it (the config's directory, unless only the working directory holds it).
    fn checkpoint(&self, e: &Value) -> Option<PathBuf> {
        let raw = Path::new(s(e.get("cdc")?, "checkpoint")?);
        let (by_cfg, by_cwd) = (self.config_dir.join(raw), self.cwd.join(raw));
        Some(if by_cwd.exists() && !by_cfg.exists() {
            by_cwd
        } else {
            by_cfg
        })
    }

    /// The OTHER local destinations this CDC stream has delivered into (recording `out` among them), each with its Success manifests: one stream, graded as the union of what it delivered.
    fn stream_dirs(&self, e: &Value, out: &Path) -> Vec<serde_json::Value> {
        let record = with_ext(&self.image(e, "dests"), "txt");
        let mut dirs: Vec<String> = std::fs::read_to_string(&record)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        let here = out.display().to_string();
        if !dirs.contains(&here) {
            dirs.push(here.clone());
            std::fs::write(&record, dirs.join("\n")).expect("record the stream's destinations");
        }
        dirs.iter()
            .filter(|d| **d != here && Path::new(d).is_dir())
            .map(|d| {
                let d = Path::new(d);
                serde_json::json!({
                    "dir": d,
                    "manifests": success_manifests(d),
                    "snapshot_dir": d.join("snapshot"),
                    "snapshot_manifests": success_manifests(&d.join("snapshot")),
                })
            })
            .collect()
    }

    /// Write the export's current source image to `<base>.parquet` (or `<base>.parquet.missing` when the table does not exist yet), and the stream's anchor when this image can say where it is; a failure leaves none.
    fn take_image(
        &self,
        e: &Value,
        envs: &[(&str, &str)],
        base: &Path,
    ) -> Option<serde_json::Value> {
        for ext in ["parquet", "parquet.missing"] {
            let _ = std::fs::remove_file(with_ext(base, ext));
        }
        let mut spec = self.facts(e, envs, &Opts::default()).ok()?;
        spec["image"] = with_ext(base, "parquet").display().to_string().into();
        spec["anchor"] = with_ext(&self.image(e, "anchor"), "parquet")
            .display()
            .to_string()
            .into();
        spec["anchor_keys"] = with_ext(&self.image(e, "anchor-keys"), "parquet")
            .display()
            .to_string()
            .into();
        spec["slot"] = e.get("cdc").and_then(|c| s(c, "slot")).into();
        spec["checkpoint"] = self.checkpoint(e).map(|p| p.display().to_string()).into();
        Some(run_rig_oracle(&spec, "image"))
    }

    /// The destination as a local directory: the configured path, or a cloud prefix pulled whole (sub-prefixes kept) through the store's own API; a `{partition}` template is cut at its partition component.
    fn local_out(&self, e: &Value) -> Result<PathBuf, String> {
        let dest = e.get("destination").ok_or("no destination")?;
        let field = if s(dest, "type") == Some("local") {
            "path"
        } else {
            "prefix"
        };
        let (root, _) = split_partition(s(dest, field).unwrap_or(""), e)?;
        if s(dest, "type") == Some("local") {
            return Ok(self.cwd.join(root));
        }
        let root = object_key_prefix(&root);
        use std::hash::{Hash as _, Hasher as _};
        let mut h = std::hash::DefaultHasher::new();
        (yaml_text(Some(dest)), &root).hash(&mut h);
        let into = self
            .config_dir
            .join(format!(".oracle_pull/{:016x}", h.finish()));
        let _ = std::fs::remove_dir_all(&into);
        let bucket = s(dest, "bucket").ok_or("a cloud destination with no bucket")?;
        match s(dest, "type") {
            Some("s3") => {
                super::storage::minio_pull_prefix(bucket, &root, &into);
            }
            Some("gcs") => match s(dest, "endpoint") {
                Some(ep) => {
                    super::storage::gcs_pull_prefix(
                        &ep.replace(":14443", ":4443"),
                        bucket,
                        &root,
                        None,
                        &into,
                    );
                }
                None => {
                    let vars = "real GCS destination: no host ADC (`gcloud auth application-default login`)";
                    let token = std::process::Command::new("gcloud")
                        .args(["auth", "application-default", "print-access-token"])
                        .output()
                        .ok()
                        .filter(|o| o.status.success())
                        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                        .ok_or(vars)?;
                    super::storage::gcs_pull_prefix(
                        "https://storage.googleapis.com",
                        bucket,
                        &root,
                        Some(&token),
                        &into,
                    );
                }
            },
            Some("azure") => {
                let ep = s(dest, "endpoint").unwrap_or(super::env::AZURITE_ENDPOINT);
                super::storage::azure_pull_prefix(ep.trim_end_matches('/'), bucket, &root, &into);
            }
            other => {
                return Err(format!(
                    "{} destination: no whole-prefix pull exists yet",
                    other.unwrap_or("?")
                ));
            }
        }
        Ok(into)
    }

    /// Success manifest names (relative to `out`) of the export's destination and its `snapshot/` leg; a `{partition}` template lists every `<col>=…` directory.
    fn manifests_of(&self, e: &Value, out: &Path) -> ManifestSnapshot {
        let dest = e.get("destination");
        let field = if dest.and_then(|d| s(d, "type")) == Some("local") {
            "path"
        } else {
            "prefix"
        };
        let template = dest.and_then(|d| s(d, field)).unwrap_or("");
        let Ok((_, Some(rest))) = split_partition(template, e) else {
            return [
                success_manifests(out),
                success_manifests(&out.join("snapshot")),
            ];
        };
        let col = format!("{}=", s(e, "partition_by").unwrap_or_default());
        let mut names = BTreeSet::new();
        for d in std::fs::read_dir(out).into_iter().flatten().flatten() {
            let part = d.file_name().to_string_lossy().to_string();
            if part.starts_with(&col) && d.path().is_dir() {
                let rel = Path::new(&part).join(rest.trim_matches('/'));
                names.extend(
                    success_manifests(&out.join(&rel))
                        .into_iter()
                        .map(|n| rel.join(n).display().to_string()),
                );
            }
        }
        [names, BTreeSet::new()]
    }
}

/// `template` cut at a whole `{partition}` component: (the part before it, the part after it when present).
/// The object-key prefix a cloud destination writes under: no empty segment (`a//b` and `/a` land at `a/b`, `a`), a trailing `/` kept.
fn object_key_prefix(prefix: &str) -> String {
    let segs: Vec<&str> = prefix.split('/').filter(|s| !s.is_empty()).collect();
    let tail = if prefix.ends_with('/') && !segs.is_empty() {
        "/"
    } else {
        ""
    };
    format!("{}{tail}", segs.join("/"))
}

fn split_partition(template: &str, e: &Value) -> Result<(String, Option<String>), String> {
    let Some(i) = template.find("{partition}") else {
        return Ok((template.to_string(), None));
    };
    let (head, tail) = (&template[..i], &template[i + "{partition}".len()..]);
    if !(head.is_empty() || head.ends_with('/'))
        || !(tail.is_empty() || tail.starts_with('/'))
        || e.get("partition_by").is_none()
    {
        return Err(
            "a `{partition}` token that is not a whole path component of a partition_by export"
                .into(),
        );
    }
    Ok((head.to_string(), Some(tail.to_string())))
}

/// Resolve the destination's `{date}`, `{export}` and `{table}` the way rivet documents them (the run's UTC date, the export name); `{run_id}` and `{partition}` stay.
fn resolve_placeholders(e: &mut Value, date: &str) {
    let name = s(e, "name").unwrap_or_default().to_string();
    let Some(dest) = e.get_mut("destination").and_then(Value::as_mapping_mut) else {
        return;
    };
    for k in ["path", "prefix"] {
        if let Some(Value::String(v)) = dest.get_mut(k) {
            *v = v
                .replace("{date}", date)
                .replace("{export}", &name)
                .replace("{table}", &name);
        }
    }
}

/// A multi-table CDC capture as one export per table, each at its own `<destination>/<table>` sub-prefix and named `<export>/<table>`; any other export unchanged.
fn per_table(e: &Value) -> Vec<Value> {
    let tables: Vec<String> = match (s(e, "mode"), e.get("tables").and_then(Value::as_sequence)) {
        (Some("cdc"), Some(t)) => t
            .iter()
            .filter_map(|t| t.as_str().map(String::from))
            .collect(),
        _ => return vec![e.clone()],
    };
    tables
        .iter()
        .map(|t| {
            let mut one = e.clone();
            let m = one.as_mapping_mut().expect("an export is a mapping");
            m.remove("tables");
            m.insert("table".into(), t.as_str().into());
            m.insert("__stream".into(), e.clone());
            // A `<table>.<column>` override names one table's column; other tables' entries are not this table's.
            if let Some(cols) = m.get_mut("columns").and_then(Value::as_mapping_mut) {
                *cols = cols
                    .iter()
                    .filter_map(|(k, v)| {
                        let k = k.as_str()?;
                        match k.rsplit_once('.') {
                            Some((tbl, col)) if tbl == t => Some((col.into(), v.clone())),
                            Some(_) => None,
                            None => Some((k.into(), v.clone())),
                        }
                    })
                    .collect();
            }
            m.insert(
                "name".into(),
                format!("{}/{t}", s(e, "name").unwrap_or("?")).into(),
            );
            if let Some(d) = one.get_mut("destination").and_then(Value::as_mapping_mut) {
                let local = d.get("type").and_then(Value::as_str) == Some("local");
                let k = if local { "path" } else { "prefix" };
                let base = d
                    .get(k)
                    .and_then(Value::as_str)
                    .unwrap_or(if local { "." } else { "" })
                    .trim_end_matches('/')
                    .to_string();
                let v = match (local, base.is_empty()) {
                    (true, _) => format!("{base}/{t}"),
                    (false, true) => format!("{t}/"),
                    (false, false) => format!("{base}/{t}/"),
                };
                d.insert(k.into(), v.into());
            }
            one
        })
        .collect()
}

/// `base` with `.ext` appended.
fn with_ext(base: &Path, ext: &str) -> PathBuf {
    PathBuf::from(format!("{}.{ext}", base.display()))
}

/// `${VAR}` in `text` resolved the way rivet resolves a config (src/config/resolve.rs): `--param` first, then the run's env; an unresolved one stays.
fn resolve_vars(text: &str, params: &[(String, String)], envs: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(i) = rest.find("${") {
        let Some(j) = rest[i..].find('}') else { break };
        let name = &rest[i + 2..i + j];
        out.push_str(&rest[..i]);
        let value = params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .or_else(|| env_of(envs, name).filter(|_| !name.is_empty()));
        out.push_str(&value.unwrap_or_else(|| rest[i..=i + j].to_string()));
        rest = &rest[i + j + 1..];
    }
    out + rest
}

/// A string field of a YAML mapping.
fn s<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

/// Whether an export's destination is the run's own stdout.
fn writes_stdout(e: &Value) -> bool {
    e.get("destination").and_then(|d| s(d, "type")) == Some("stdout")
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

/// Run one oracle verb over `spec`; log PASS / SKIP / XFAIL, or panic with every disagreement. Returns whether it XFAILed.
fn verdict_of(name: &str, spec: &serde_json::Value, verb: &str, opts: &Opts) -> bool {
    let t0 = std::time::Instant::now();
    // A marker names one export and one failure class; the oracle says whether every failure is of it.
    let marker = opts.xfail.filter(|k| {
        k.export == name && verb == "grade" && Leftover::of_known_defect_class(k.class).is_none()
    });
    let mut spec = spec.clone();
    if let Some(k) = marker {
        spec["known_defect"] = k.class.into();
    }
    let spec = &spec;
    let verdict = run_rig_oracle(spec, verb);
    let took = format!("{verb} {} ms", t0.elapsed().as_millis());
    match decide(&verdict, marker, spec["state_missing"].as_str()) {
        Outcome::Skip(why) => log("SKIP", name, &format!("{took}: {why}")),
        Outcome::Pass(facts) => log("PASS", name, &format!("{took} {facts}")),
        Outcome::Partial(detail) => log("PARTIAL", name, &format!("{took}: {detail}")),
        Outcome::Xfail(detail) => {
            log("XFAIL", name, &detail);
            return true;
        }
        Outcome::Fail(failures) => {
            log("FAIL", name, &failures.join(" | "));
            panic!(
                "rig oracle: export '{name}' disagrees with its source / rivet's own ledger \
                 (dev/release_oracle/rig_oracle.py {verb}; opt out only with `.no_oracle(\"<why>\")` or \
                 {NO_ORACLE_ENV}=<why>):\n  - {}\nspec: {spec}\nverdict: {verdict}",
                failures.join("\n  - ")
            );
        }
    }
    false
}

/// What one oracle verdict means for the test: the verdict word and its log detail.
#[derive(Debug, PartialEq)]
enum Outcome {
    Skip(String),
    Pass(String),
    Partial(String),
    Xfail(String),
    Fail(Vec<String>),
}

/// The oracle's JSON read against its contract (`{skip: str}` | `{failures: [str], partial?: str, known_defect?: bool}`): any other shape panics as an oracle error, never a PASS.
fn decide(
    verdict: &serde_json::Value,
    marker: Option<KnownDefect>,
    state_missing: Option<&str>,
) -> Outcome {
    let obj = verdict
        .as_object()
        .unwrap_or_else(|| panic!("oracle verdict is not a JSON object: {verdict}"));
    if let Some(skip) = obj.get("skip") {
        let why = skip
            .as_str()
            .unwrap_or_else(|| panic!("oracle verdict `skip` is not a string: {verdict}"));
        return Outcome::Skip(why.to_string());
    }
    let failures: Vec<String> = match obj.get("failures").and_then(|f| f.as_array()) {
        Some(items) => items
            .iter()
            .map(|f| {
                f.as_str().map(str::to_string).unwrap_or_else(|| {
                    panic!("oracle verdict `failures` holds a non-string finding: {f} in {verdict}")
                })
            })
            .collect(),
        None => panic!(
            "oracle verdict has no `failures` array (the contract is `skip: str` or `failures: [str]`): {verdict}"
        ),
    };
    let partial = obj.get("partial").map(|p| {
        p.as_str()
            .unwrap_or_else(|| panic!("oracle verdict `partial` is not a string: {verdict}"))
            .to_string()
    });
    let facts = obj.get("facts").cloned().unwrap_or(serde_json::Value::Null);
    if failures.is_empty() {
        let gaps: Vec<String> = state_missing
            .map(str::to_string)
            .into_iter()
            .chain(partial)
            .collect();
        return if gaps.is_empty() {
            Outcome::Pass(facts.to_string())
        } else {
            Outcome::Partial(format!("{} {facts}", gaps.join("; ")))
        };
    }
    if let Some(k) = marker {
        let covered = match obj.get("known_defect") {
            Some(v) => v.as_bool().unwrap_or_else(|| {
                panic!("oracle verdict `known_defect` is not a bool: {verdict}")
            }),
            None => panic!(
                "the spec named known defect class `{}` and the oracle's verdict carries no `known_defect` bool: {verdict}",
                k.class
            ),
        };
        if covered {
            return Outcome::Xfail(format!(
                "[{}] {} — {}",
                k.class,
                k.reason,
                failures.join(" | ")
            ));
        }
    }
    Outcome::Fail(failures)
}

/// The uv-synced interpreter, resolved once: `uv run` reports a Python killed by a signal as a bare exit 1.
fn oracle_python() -> &'static str {
    static PY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PY.get_or_init(|| {
        let out = std::process::Command::new("uv")
            .args([
                "run",
                "--frozen",
                "--quiet",
                "python",
                "-c",
                "import sys; print(sys.executable)",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect(
                "spawn `uv run` for the rig oracle — install uv (the oracle is pinned by uv.lock)",
            );
        assert!(
            out.status.success(),
            "uv could not resolve the rig oracle's interpreter ({}):\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    })
}

/// Run `dev/release_oracle/rig_oracle.py <verb>` (pinned by uv.lock) over `spec` within `TIMEOUT_SECS`; its JSON verdict.
fn run_rig_oracle(spec: &serde_json::Value, verb: &str) -> serde_json::Value {
    use std::io::{Read as _, Write as _};
    use std::os::unix::process::CommandExt as _;
    const TIMEOUT_SECS: u64 = 300;
    let mut child = std::process::Command::new(oracle_python())
        .args(["-m", "dev.release_oracle.rig_oracle", verb])
        .env("PYTHONFAULTHANDLER", "1")
        .process_group(0)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn the rig oracle's interpreter");
    child
        .stdin
        .take()
        .expect("oracle stdin")
        .write_all(spec.to_string().as_bytes())
        .expect("write the oracle spec");
    let drain = |mut r: Box<dyn std::io::Read + Send>| {
        std::thread::spawn(move || {
            let mut b = Vec::new();
            let _ = r.read_to_end(&mut b);
            b
        })
    };
    let stdout = drain(Box::new(child.stdout.take().expect("oracle stdout")));
    let stderr = drain(Box::new(child.stderr.take().expect("oracle stderr")));
    let started = std::time::Instant::now();
    let group = child.id() as i32;
    let mut timed_out = false;
    let status = loop {
        if let Some(st) = child.try_wait().expect("poll the rig oracle") {
            break st;
        }
        if !timed_out && started.elapsed().as_secs() >= TIMEOUT_SECS {
            // SIGABRT first: Python's faulthandler prints every thread's stack, then the group dies.
            timed_out = true;
            unsafe { libc::kill(-group, libc::SIGABRT) };
            std::thread::sleep(std::time::Duration::from_secs(2));
            unsafe { libc::kill(-group, libc::SIGKILL) };
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    let out = stdout.join().unwrap_or_default();
    let err = String::from_utf8_lossy(&stderr.join().unwrap_or_default()).into_owned();
    if timed_out || !status.success() {
        let why = if timed_out {
            format!("timed out after {TIMEOUT_SECS}s and was killed")
        } else {
            format!(
                "exited {status} after {:.1}s",
                started.elapsed().as_secs_f64()
            )
        };
        log(
            "FAIL",
            spec["export"].as_str().unwrap_or("*"),
            &format!(
                "oracle error ({verb}): {why}: {}",
                err.lines().last().unwrap_or("<empty stderr>")
            ),
        );
        panic!(
            "oracle error: dev/release_oracle/rig_oracle.py {verb} {why} (an oracle bug, never a verdict)\n\
             stderr:\n{err}\nstdout:\n{}\nspec: {spec}",
            String::from_utf8_lossy(&out)
        );
    }
    serde_json::from_slice(&out).unwrap_or_else(|e| {
        panic!(
            "rig oracle printed no JSON verdict ({e}):\n{}",
            String::from_utf8_lossy(&out)
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

#[test]
fn the_oracle_pulls_the_key_prefix_a_doubled_slash_lands_at() {
    assert_eq!(
        object_key_prefix("rivet-live/u/my//users/"),
        "rivet-live/u/my/users/"
    );
    assert_eq!(object_key_prefix("/a//b"), "a/b");
    assert_eq!(object_key_prefix("a/b/"), "a/b/");
    assert_eq!(object_key_prefix("/"), "");
}

#[test]
fn the_oracle_resolves_config_vars_as_rivet_does_params_first() {
    let params = [("T".to_string(), "from_param".to_string())];
    let envs = [("T", "from_env"), ("U", "url_env")];
    assert_eq!(
        resolve_vars(
            "q: ${T} url: ${U} keep: ${RIVET_ORACLE_UNSET_VAR}",
            &params,
            &envs
        ),
        "q: from_param url: url_env keep: ${RIVET_ORACLE_UNSET_VAR}",
    );
}

#[test]
fn the_oracle_resolves_destination_placeholders_and_splits_a_capture_per_table() {
    let mut e: Value = serde_yaml_ng::from_str(
        "name: orders\nmode: cdc\ntables: [public.a, b]\ncolumns: {public.a.v: int8, b.w: text, x: text}\n\
         destination: {type: gcs, bucket: k, prefix: 'runs/{date}/{export}/{table}/{run_id}'}",
    )
    .unwrap();
    resolve_placeholders(&mut e, "2026-10-01");
    assert_eq!(
        s(&e["destination"], "prefix"),
        Some("runs/2026-10-01/orders/orders/{run_id}")
    );
    let [a, b]: [Value; 2] = per_table(&e).try_into().unwrap();
    assert_eq!(
        (s(&a, "name"), s(&a, "table")),
        (Some("orders/public.a"), Some("public.a"))
    );
    assert_eq!(
        s(&b["destination"], "prefix"),
        Some("runs/2026-10-01/orders/orders/{run_id}/b/")
    );
    assert_eq!(yaml_text(a.get("columns")), "v: int8\nx: text\n");
    let p: Value = serde_yaml_ng::from_str("partition_by: d").unwrap();
    assert_eq!(
        split_partition("o/{partition}/e/", &p),
        Ok(("o/".into(), Some("/e/".into())))
    );
    assert!(
        split_partition("o/x{partition}", &p).is_err(),
        "a token inside a component is not a hive directory"
    );
}

/// A marker for the verdict-reader tests: one export, one class.
fn a_marker() -> KnownDefect<'static> {
    KnownDefect {
        export: "e",
        class: "delivered-only rows",
        reason: "a probe marker",
    }
}

#[test]
fn the_verdict_reader_grades_the_contract_shapes() {
    use serde_json::json;
    assert_eq!(
        decide(&json!({"failures": [], "facts": {"n": 1}}), None, None),
        Outcome::Pass(r#"{"n":1}"#.into())
    );
    assert_eq!(
        decide(
            &json!({"failures": ["COUNT(*): source 1, delivered 2"]}),
            None,
            None
        ),
        Outcome::Fail(vec!["COUNT(*): source 1, delivered 2".into()])
    );
    assert_eq!(
        decide(&json!({"skip": "no stand"}), None, None),
        Outcome::Skip("no stand".into())
    );
    assert!(matches!(
        decide(&json!({"failures": [], "partial": "the stream's first graded run"}), None, None),
        Outcome::Partial(d) if d.starts_with("the stream's first graded run")
    ));
    assert!(
        matches!(
            decide(&json!({"failures": []}), None, Some("no state DB: x is absent")),
            Outcome::Partial(d) if d.starts_with("no state DB: x is absent")
        ),
        "a run with no findable state DB is PARTIAL, not PASS"
    );
}

#[test]
fn a_known_defect_excuses_only_the_failures_the_oracle_marks_as_its_class() {
    use serde_json::json;
    let of_class = json!({"failures": ["COUNT(*): source 1, delivered 2"], "known_defect": true});
    assert!(matches!(
        decide(&of_class, Some(a_marker()), None),
        Outcome::Xfail(_)
    ));
    let other = json!({"failures": ["COUNT(*): source 2, delivered 1"], "known_defect": false});
    assert!(
        matches!(decide(&other, Some(a_marker()), None), Outcome::Fail(_)),
        "a failure outside the marked class FAILs even with a marker"
    );
    assert!(
        matches!(decide(&of_class, None, None), Outcome::Fail(_)),
        "without a marker the oracle's bool excuses nothing"
    );
}

#[test]
#[should_panic(expected = "non-string finding")]
fn an_object_finding_is_an_oracle_error_not_a_pass() {
    decide(
        &serde_json::json!({"failures": [{"line": "x"}]}),
        None,
        None,
    );
}

#[test]
#[should_panic(expected = "no `failures` array")]
fn a_renamed_failures_key_is_an_oracle_error_not_a_pass() {
    decide(
        &serde_json::json!({"failure": ["x"], "facts": {}}),
        None,
        None,
    );
}

#[test]
#[should_panic(expected = "no `failures` array")]
fn a_non_array_failures_is_an_oracle_error_not_a_pass() {
    decide(&serde_json::json!({"failures": "x"}), None, None);
}

#[test]
#[should_panic(expected = "`known_defect` is not a bool")]
fn a_non_bool_known_defect_is_an_oracle_error() {
    decide(
        &serde_json::json!({"failures": ["x"], "known_defect": "yes"}),
        Some(a_marker()),
        None,
    );
}

#[test]
#[should_panic(expected = "carries no `known_defect` bool")]
fn a_marked_spec_whose_verdict_lost_the_known_defect_key_is_an_oracle_error() {
    decide(
        &serde_json::json!({"failures": ["x"]}),
        Some(a_marker()),
        None,
    );
}
