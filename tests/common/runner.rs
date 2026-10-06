//! Driving the `rivet` / `rivet-mcp` binaries from integration tests, plus
//! light helpers for writing the YAML config and discovering produced files.

#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Command, Output};

/// Absolute path to the `rivet` binary built for this integration test.
pub const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// The binary the tests actually run — `RIVET_BIN`, unless `RIVET_BIN_OVERRIDE`
/// names another one.
///
/// Exists for exactly one job: measuring the CURRENT tree against a PREVIOUS
/// RELEASE. The comparison has to run the same fixture through both, and a release
/// binary cannot be reached through `CARGO_BIN_EXE_rivet`, which cargo fixes at
/// compile time. Downloading the published artifact (never rebuilding the parent —
/// the release profile is fat-LTO and each build is minutes) is the documented way
/// to get the other side.
///
/// LOUD by construction: an override that a later run forgets about would grade the
/// wrong binary while every signal says the tests passed, which is the staleness
/// class this repo has already paid for twice. So it announces itself on stderr the
/// first time it is read, and [`rivet_bin_label`] puts the version into the
/// measurement's own output — the place a reader is actually looking.
pub fn rivet_bin() -> &'static str {
    static RESOLVED: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    RESOLVED.get_or_init(|| match std::env::var("RIVET_BIN_OVERRIDE") {
        Ok(p) if !p.is_empty() => {
            assert!(
                std::path::Path::new(&p).is_file(),
                "RIVET_BIN_OVERRIDE={p} is not a file — a typo here silently grades \
                 the wrong binary, so it is refused rather than ignored"
            );
            eprintln!("NOTE: RIVET_BIN_OVERRIDE is set — running {p}, NOT this tree's build");
            p
        }
        _ => RIVET_BIN.to_string(),
    })
}

/// `rivet --version` of whatever [`rivet_bin`] resolved to, for a measurement to
/// print. A number without the binary that produced it is not a comparison.
pub fn rivet_bin_label() -> String {
    let v = rivet_command(&["--version"], &[])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();
    let v = v.trim().to_string();
    if std::env::var("RIVET_BIN_OVERRIDE").is_ok_and(|p| !p.is_empty()) {
        format!("{v} (override)")
    } else {
        format!("{v} (this tree)")
    }
}

/// Absolute path to the `rivet-mcp` binary built for this integration test.
pub const RIVET_MCP_BIN: &str = env!("CARGO_BIN_EXE_rivet-mcp");

/// Write `yaml` to `<tmpdir>/rivet.yaml` and return the path.  The tempdir is
/// returned too so the caller can keep it alive for the duration of the run.
pub fn write_config(tmpdir: &tempfile::TempDir, yaml: &str) -> PathBuf {
    let path = tmpdir.path().join("rivet.yaml");
    std::fs::write(&path, yaml).expect("write rivet config");
    path
}

/// A `rivet` command with every inherited `RIVET_*` stripped; the child sees only the state backend under test and `envs`.
pub fn rivet_command(args: &[impl AsRef<std::ffi::OsStr>], envs: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(rivet_bin());
    cmd.args(args);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("RIVET_") {
            cmd.env_remove(&k);
        }
    }
    note_ignored_shell_state_url();
    if let Some(url) = super::state::state_url_under_test() {
        cmd.env("RIVET_STATE_URL", url);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd
}

/// Say once per process that a shell `RIVET_STATE_URL` does not reach rivet; the harness knob is `RIVET_GATE_STATE_URL`.
fn note_ignored_shell_state_url() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let shell = std::env::var("RIVET_STATE_URL").is_ok_and(|u| u.starts_with("postgres"));
        if shell && super::state::state_url_under_test().is_none() {
            eprintln!(
                "NOTE: RIVET_STATE_URL is set in the shell but stripped from every rivet the \
                 harness spawns; set RIVET_GATE_STATE_URL to grade Postgres state"
            );
        }
    });
}

/// Run `rivet <args...>` and capture stdout/stderr.  Panics if the process
/// cannot be spawned (which indicates a build-time problem, not a test
/// failure).
pub fn run_rivet(args: &[&str]) -> Output {
    run_rivet_env(args, &[])
}

/// Run `spawn` and hand a successful `run|load|compact --config` to the default oracle (verify.rs).
fn graded(
    args: &[&str],
    envs: &[(&str, &str)],
    cwd: Option<&std::path::Path>,
    spawn: impl FnOnce() -> Output,
) -> Output {
    let argv: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let case = super::verify::begin_raw(&argv, envs, cwd);
    let out = spawn();
    if let Some(mut case) = case.filter(|_| out.status.success()) {
        case.delivered(&out.stdout);
        super::verify::finish(case, envs, &Default::default());
    }
    out
}

/// `run_rivet` with extra environment variables (fault hooks, log levels).
pub fn run_rivet_env(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = rivet_command(args, envs);
    graded(args, envs, None, || {
        cmd.output().expect("spawn rivet binary")
    })
}

/// `run_rivet_env` with the process working directory set to `dir`.
///
/// A config `rivet init` GENERATED carries a relative `destination.path:`
/// (`./output/<table>/`), and `LocalDestination` keeps that string verbatim —
/// so it resolves against the process CWD, unlike `cdc.checkpoint:`, which the
/// runtime resolves through the config's own directory. Running a generated
/// config the way its next-steps text tells an operator to — from the directory
/// holding it — is therefore only expressible with the CWD set.
pub fn run_rivet_in_dir(dir: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = rivet_command(args, envs);
    cmd.current_dir(dir);
    graded(args, envs, Some(dir), || {
        cmd.output().expect("spawn rivet binary")
    })
}

/// Spawn `rivet run --config <cfg>` and wait up to `timeout` for it to exit on
/// its own. Returns the elapsed time if it terminated within the budget (and
/// asserts a clean exit), or `None` if it had to be killed. This is what makes a
/// `until_current`-terminates-under-load assertion possible without risking a
/// suite-wide hang: a drain loop that never reaches its stop condition fails the
/// test (returns `None`) instead of blocking `output()` forever.
pub fn run_rivet_bounded(
    cfg: &std::path::Path,
    timeout: std::time::Duration,
) -> Option<std::time::Duration> {
    let start = std::time::Instant::now();
    let argv = ["run", "--config", cfg.to_str().unwrap()];
    let case = super::verify::begin_raw(&argv.map(String::from), &[], None);
    let mut child = rivet_command(&argv, &[])
        .spawn()
        .expect("spawn rivet binary");
    loop {
        if let Some(status) = child.try_wait().expect("try_wait rivet") {
            assert!(status.success(), "bounded rivet run exited non-zero");
            let took = start.elapsed();
            if let Some(case) = case {
                super::verify::finish(case, &[], &Default::default());
            }
            return Some(took);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Like `run_rivet` but sets `RUST_LOG=warn` so that `log::warn!` output is
/// visible in stderr.  Use this when a test needs to assert on warning messages
/// emitted via the log crate (plan validation warnings, quality warnings, etc.).
pub fn run_rivet_with_warn_log(args: &[&str]) -> Output {
    graded(args, &[], None, || {
        rivet_command(args, &[("RUST_LOG", "warn")])
            .output()
            .expect("spawn rivet binary")
    })
}

/// Convenience: `rivet run --config <path> --export <name>` and return the
/// captured output.  Caller is responsible for asserting exit code / contents.
pub fn run_rivet_export(config_path: &std::path::Path, export_name: &str) -> Output {
    run_rivet(&[
        "run",
        "--config",
        config_path.to_str().unwrap(),
        "--export",
        export_name,
    ])
}

/// Collect every file with the given extension under `dir` (non-recursive).
/// Useful for locating the timestamped output rivet just wrote.
pub fn files_with_extension(dir: &std::path::Path, ext: &str) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return vec![];
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|e| e == ext))
        .map(|e| e.path())
        .collect()
}

/// Every file under `dir`, at any depth (empty when `dir` does not exist).
pub fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return vec![];
    };
    rd.filter_map(Result::ok)
        .flat_map(|e| {
            let p = e.path();
            if p.is_dir() { files_under(&p) } else { vec![p] }
        })
        .collect()
}

/// A refused CDC run with `initial: snapshot` wrote nothing: no part, no manifest, no marker, no checkpoint.
pub fn assert_refused_before_any_write(out: &std::path::Path, ckpt: &std::path::Path) {
    let written = files_under(out);
    assert!(
        written.is_empty(),
        "a refused run must write no snapshot part, manifest or marker: {written:?}"
    );
    assert!(
        !ckpt.exists(),
        "a refused run must write no checkpoint: {}",
        ckpt.display()
    );
}

/// Like [`run_rivet_bounded`], but for arbitrary CLI args (e.g. the `rivet cdc`
/// NDJSON driver) with stdout captured — `Some(stdout)` on clean exit within
/// the ceiling, `None` if it had to be killed (the caller asserts on that).
pub fn run_rivet_args_bounded(args: &[&str], timeout: std::time::Duration) -> Option<String> {
    run_rivet_args_bounded_env(args, &[], timeout)
}

/// As [`run_rivet_args_bounded`], with environment for the child — the
/// credential-safety forms (`--source-env`) cannot be exercised without it.
pub fn run_rivet_args_bounded_env(
    args: &[&str],
    envs: &[(&str, &str)],
    timeout: std::time::Duration,
) -> Option<String> {
    let dir = tempfile::tempdir().expect("stdout tempdir");
    let path = dir.path().join("stdout");
    let stdout = std::fs::File::create(&path).expect("stdout capture file");
    let start = std::time::Instant::now();
    let mut cmd = rivet_command(args, envs);
    cmd.stdout(stdout);
    let argv: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let case = super::verify::begin_raw(&argv, envs, None);
    let mut child = cmd.spawn().expect("spawn rivet binary");
    loop {
        if let Some(status) = child.try_wait().expect("try_wait rivet") {
            assert!(status.success(), "bounded rivet run exited non-zero");
            let stdout = std::fs::read_to_string(&path).expect("read captured stdout");
            if let Some(mut case) = case {
                case.delivered(stdout.as_bytes());
                super::verify::finish(case, envs, &Default::default());
            }
            return Some(stdout);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// First-column integer keys of the `after` images a `rivet cdc` NDJSON run printed for `table`; a line it cannot read is a panic, never a skipped id.
pub fn ndjson_after_ids(stdout: &str, table: &str) -> std::collections::BTreeSet<i64> {
    let lines = stdout.lines().filter(|l| !l.trim().is_empty());
    lines
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l)
                .unwrap_or_else(|e| panic!("`rivet cdc` stdout line is not JSON ({e}): {l}"))
        })
        .filter(|v| v["table"].as_str() == Some(table) && !v["after"].is_null())
        .map(|v| {
            let key = &v["after"][0];
            ndjson_key(key).unwrap_or_else(|| panic!("no integer key in after[0]: {v}"))
        })
        .collect()
}

/// An NDJSON key cell as an integer: a JSON number (SQL engines) or its decimal text (MongoDB's flat `_id`).
fn ndjson_key(key: &serde_json::Value) -> Option<i64> {
    key.as_i64().or_else(|| key.as_str()?.parse().ok())
}

#[cfg(test)]
mod ndjson_tests {
    use super::ndjson_after_ids;

    #[test]
    fn ndjson_after_ids_reads_numeric_and_text_keys_of_one_table() {
        let out = concat!(
            r#"{"table":"t","after":[1,"a"]}"#,
            "\n",
            r#"{"table":"t","after":["2","{}"]}"#,
            "\n",
            r#"{"table":"t","before":[3],"after":null}"#,
            "\n",
            r#"{"table":"other","after":[9]}"#,
            "\n",
        );
        assert_eq!(ndjson_after_ids(out, "t"), [1, 2].into());
    }

    #[test]
    #[should_panic(expected = "no integer key in after[0]")]
    fn ndjson_after_ids_refuses_a_key_it_cannot_read() {
        ndjson_after_ids(r#"{"table":"t","after":["abc"]}"#, "t");
    }

    #[test]
    #[should_panic(expected = "is not JSON")]
    fn ndjson_after_ids_refuses_a_line_that_is_not_json() {
        ndjson_after_ids("done: 2 events", "t");
    }
}
