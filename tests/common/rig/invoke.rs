//! INVOKE — the single execution seam. Every method that reaches the rivet
//! binary bottoms out in run_argv/cli_argv -> invoke_command -> invoke; the
//! CDC conformance gate DERIVES its capture markers from the `pub fn`
//! run*/cli*/spawn* names in this file (cdc_conformance_gate.rs), so a new
//! runner method is born graded.

use super::*;

impl Rig {
    /// argv for a `run` invocation: `run --config <cfg> <extra…>`.
    fn run_argv(&self, extra: &[&str]) -> Vec<String> {
        let cfg = self.config_path();
        let mut v = vec![
            "run".to_string(),
            "--config".to_string(),
            cfg.display().to_string(),
        ];
        v.extend(extra.iter().map(|a| a.to_string()));
        v
    }

    /// argv for a non-`run` subcommand: `<args…> --config <cfg>` (clap
    /// accepts the flag in any position).
    fn cli_argv(&self, args: &[&str]) -> Vec<String> {
        let cfg = self.config_path();
        let mut v: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        v.push("--config".to_string());
        v.push(cfg.display().to_string());
        v
    }

    /// The single `Command` builder behind every runner wrapper.
    fn invoke_command(&self, argv: &[String], envs: &[(&str, &str)]) -> std::process::Command {
        crate::common::runner::rivet_command(argv, envs)
    }

    /// Run to completion and collect the output.
    fn invoke(&self, argv: &[String], envs: &[(&str, &str)]) -> std::process::Output {
        self.invoke_in(argv, envs, None)
    }

    /// The one blocking bracket: snapshot, run (in `cwd` when given), absorb config writes, grade by the exit.
    fn invoke_in(
        &self,
        argv: &[String],
        envs: &[(&str, &str)],
        cwd: Option<&std::path::Path>,
    ) -> std::process::Output {
        let mut case = self.oracle_begin(argv, envs, cwd);
        let mut cmd = self.invoke_command(argv, envs);
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
        let (pid, out) = crate::common::runner::output_of(&mut cmd);
        if let Some(case) = case.as_mut() {
            case.ran_as(pid);
        }
        // rivet itself may write into the config (`plan` annotates wave-less
        // configs even without --annotate-waves) — absorb it so the hand-edit
        // guard keeps firing only on edits made OUTSIDE an invocation.
        self.absorb_product_config_writes();
        self.oracle_settle(case, out.status, &out.stdout, envs);
        out
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
        self.invoke(&self.cli_argv(args), envs)
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
        let mut argv: Vec<String> = vec![
            "apply".into(),
            plan.to_str().expect("plan path must be utf-8").into(),
        ];
        argv.extend(extra.iter().map(|s| s.to_string()));
        // Through `invoke`, like every other runner: the harness audit found this
        // method routed around invoke_command (via runner::run_rivet_env), which
        // both falsified the module header's "every method bottoms out in invoke"
        // claim and skipped absorb_product_config_writes — an apply that touched
        // the config would false-trip the hand-edit guard at the next
        // materialization. `apply` EXECUTES an export, so it is a capture; the
        // conformance gate's derivation now includes the `apply*` prefix.
        self.invoke(&argv, envs)
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
        self.invoke(&self.run_argv(extra), envs)
    }

    /// `run_args_env` with no env — a plain run with extra flags.
    pub fn run_args(&self, extra: &[&str]) -> std::process::Output {
        self.run_args_env(extra, &[])
    }

    /// `rivet load --config <cfg>` plus `extra` flags.
    fn load_argv(&self, extra: &[&str]) -> Vec<String> {
        let cfg = self.config_path();
        let mut v = vec![
            "load".to_string(),
            "--config".to_string(),
            cfg.display().to_string(),
        ];
        v.extend(extra.iter().map(|a| a.to_string()));
        v
    }

    /// `rivet load` with extra flags and environment — the load leg's
    /// counterpart to [`Rig::run_args_env`].
    ///
    /// Warehouse tests were each spelling `cli_env(&["load", …])`, which is the
    /// per-file command wrapper the rig exists to remove; `RIVET_STATE_URL` is
    /// an env var, so the env-carrying form is the one that has to exist.
    pub fn load_args_env(&self, extra: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
        self.invoke(&self.load_argv(extra), envs)
    }

    /// `load_args_env` with no env — a plain load with extra flags.
    pub fn load_args(&self, extra: &[&str]) -> std::process::Output {
        self.load_args_env(extra, &[])
    }

    /// Load and assert it succeeded, surfacing stderr on failure.
    pub fn load_ok(&self, extra: &[&str], envs: &[(&str, &str)]) {
        let out = self.load_args_env(extra, envs);
        assert!(
            out.status.success(),
            "rig load failed for '{}':\n{}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Run with the child's WORKING DIRECTORY set — the seam for path-resolution
    /// contracts.
    ///
    /// A relative path in a config is resolved by SOMETHING, and which something is
    /// a contract worth testing: `cdc.checkpoint: ./x.ckpt` was resolved against the
    /// process CWD until round 9 measured the cost (the same config run from a cron
    /// entry and from a shell looked in two places, found nothing the second time,
    /// and re-anchored at the current log position — three green runs delivered
    /// `[3]` of `[1,2,3]`). Every other rig entry point inherits the test harness's
    /// own directory, so nothing could express the case; two hand-rolled
    /// `Command::new(RIVET_BIN)` sites did, and the rig-adoption guard rightly
    /// refused them.
    pub fn run_in_dir(&self, dir: &std::path::Path) -> std::process::Output {
        self.invoke_in(&self.run_argv(&[]), &[], Some(dir))
    }

    /// [`Rig::run_in_dir`] for any OTHER subcommand — `doctor`, `check`, `validate`.
    ///
    /// A diagnostic must answer about the file the RUN will open. `rivet doctor`
    /// resolved `cdc.checkpoint:` against the process working directory while the
    /// run resolved it against the config's, so the same config graded green from
    /// one shell and described a different file from another — and the ABSENT
    /// answer is this check's green one ("no checkpoint yet — the first run pins
    /// the open position"). `run_in_dir` could express the run half of that pair
    /// and nothing could express the diagnostic half.
    pub fn cli_in_dir(&self, args: &[&str], dir: &std::path::Path) -> std::process::Output {
        self.invoke_in(&self.cli_argv(args), &[], Some(dir))
    }

    /// Run with an extra environment variable (fault injection); returns the
    /// raw output — the caller asserts success or failure.
    pub fn run_with_env(&self, key: &str, val: &str) -> std::process::Output {
        self.run_args_env(&[], &[(key, val)])
    }

    /// Run with SEVERAL extra environment variables (e.g. RIVET_STATE_URL to pick the
    /// Postgres state backend AND RIVET_TEST_PANIC_AT to inject a crash in one run);
    /// returns the raw output — the caller asserts success or failure.
    pub fn run_with_envs(&self, envs: &[(&str, &str)]) -> std::process::Output {
        self.run_args_env(&[], envs)
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
        let out_path = self.dir.path().join("bounded.stdout");
        let err_path = self.dir.path().join("bounded.stderr");
        let mut cmd = self.invoke_command(&self.run_argv(&[]), envs);
        cmd.stdout(std::fs::File::create(&out_path).expect("bounded stdout file"))
            .stderr(std::fs::File::create(&err_path).expect("bounded stderr file"));
        let case = self.oracle_begin(&self.run_argv(&[]), envs, None);
        let mut child = cmd.spawn().expect("spawn rivet binary");
        let start = std::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("try_wait rivet") {
                self.absorb_product_config_writes();
                let out = std::process::Output {
                    status,
                    stdout: std::fs::read(&out_path).unwrap_or_default(),
                    stderr: std::fs::read(&err_path).unwrap_or_default(),
                };
                self.oracle_settle(case, out.status, &out.stdout, envs);
                return Some(out);
            }
            if start.elapsed() >= timeout {
                let _ = child.kill();
                let _ = child.wait();
                if let Some(case) = &case {
                    case.ungraded("killed at its wall-clock ceiling");
                }
                // Absorb on the KILL path too: a product config write that
                // landed before the timeout would otherwise false-trip the
                // hand-edit guard at the next materialization.
                self.absorb_product_config_writes();
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// Spawn `rivet run` and hand back the LIVE child, output discarded.
    ///
    /// For tests that must act on a running process — signal it, inspect its
    /// children, watch the staged `.tmp` appear — rather than wait for an exit
    /// status. `run_args_env` blocks until completion and so cannot express them.
    /// The caller reaps it through [`Spawned`]; the reaping grades the run by its exit like any other.
    pub fn spawn_args_env(&self, extra: &[&str], envs: &[(&str, &str)]) -> Spawned<'_> {
        let argv = self.run_argv(extra);
        let mut case = self.oracle_begin(&argv, envs, None);
        let child = self
            .invoke_command(&argv, envs)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rivet");
        if let Some(case) = case.as_mut() {
            case.ran_as(child.id());
        }
        Spawned {
            rig: self,
            child,
            case,
            envs: envs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// Run rivet; panic unless it succeeds.
    pub fn run_ok(&self) {
        let out = self.run_args(&[]);
        assert!(
            out.status.success(),
            "rig run failed for '{}':\n{}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Run rivet; panic unless it succeeds, and return what it SAID.
    ///
    /// The oracle for a WARNING: a warning by definition rides on a successful run,
    /// so `run_ok` (which throws the output away) and `run_expect_fail` (which
    /// demands a non-zero exit) both grade the wrong thing. Tests that hand-rolled
    /// `Command::new(RIVET_BIN)` to read stderr off a green run are what this
    /// replaces.
    pub fn run_ok_capture(&self) -> String {
        let out = self.run_args(&[]);
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.status.success(),
            "rig run failed for '{}':\n{said}",
            self.name
        );
        said
    }

    /// Run rivet expecting a loud failure; returns combined output.
    pub fn run_expect_fail(&self) -> String {
        let out = self.run_args(&[]);
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
        self.run_args(&[])
    }

    /// [`Rig::run_args_env`] while the cdc-standby primary logs a running-transactions snapshot every 300 ms (what a slot created on its standby waits for).
    pub fn run_nudged(&self, envs: &[(&str, &str)]) -> std::process::Output {
        use std::sync::atomic::{AtomicBool, Ordering};
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let nudger = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut c = postgres::Client::connect(
                    crate::common::env::PG_STANDBY_PRIMARY_URL,
                    postgres::NoTls,
                )
                .expect("connect the cdc-standby primary");
                while !stop.load(Ordering::Relaxed) {
                    let _ = c.execute("SELECT pg_log_standby_snapshot()", &[]);
                    std::thread::sleep(std::time::Duration::from_millis(300));
                }
            })
        };
        let out = self.run_args_env(&[], envs);
        stop.store(true, Ordering::Relaxed);
        nudger.join().expect("the standby nudger");
        out
    }

    /// `rivet doctor --json`, then the run it predicts: panics when doctor reported all_ok and the run refused; returns (doctor's all_ok, the run).
    pub fn run_after_doctor(&self) -> (bool, std::process::Output) {
        let doctor = self.cli(&["doctor", "--json"]);
        let run = self.run();
        match doctor_agrees_with_run(&doctor.stdout, run.status.success()) {
            Ok(green) => (green, run),
            Err(why) => panic!(
                "{why} — rig '{}', run stderr:\n{}",
                self.name,
                String::from_utf8_lossy(&run.stderr)
            ),
        }
    }
}

/// Doctor's all_ok when the run after it agrees, else why not (all_ok, then a refused run); an unreadable report is an error.
pub(crate) fn doctor_agrees_with_run(report: &[u8], run_ok: bool) -> Result<bool, String> {
    let v: serde_json::Value = serde_json::from_slice(report).unwrap_or_else(|e| {
        panic!(
            "`rivet doctor --json` printed no JSON report ({e}):\n{}",
            String::from_utf8_lossy(report)
        )
    });
    let green = v["all_ok"]
        .as_bool()
        .unwrap_or_else(|| panic!("the doctor report has no `all_ok` bool: {v}"));
    if green && !run_ok {
        return Err(format!(
            "doctor and run disagree: doctor reported all_ok, the run refused; doctor: {v}"
        ));
    }
    Ok(green)
}

#[test]
fn a_green_doctor_followed_by_a_refused_run_is_a_disagreement() {
    let green = br#"{"all_ok": true, "checks": []}"#;
    let red = br#"{"all_ok": false, "checks": []}"#;
    assert!(doctor_agrees_with_run(green, false).is_err());
    assert_eq!(doctor_agrees_with_run(green, true), Ok(true));
    assert_eq!(doctor_agrees_with_run(red, false), Ok(false));
    assert_eq!(doctor_agrees_with_run(red, true), Ok(false));
}

#[test]
#[should_panic(expected = "no `all_ok` bool")]
fn a_doctor_report_without_all_ok_is_an_error_not_agreement() {
    let _ = doctor_agrees_with_run(br#"{"ok": true}"#, false);
}

/// A live `rivet run` child of [`Rig::spawn_args_env`]: a `Child` (by deref) whose reaping grades the run by its exit.
pub struct Spawned<'a> {
    rig: &'a Rig,
    child: std::process::Child,
    case: Option<crate::common::verify::Case>,
    envs: Vec<(String, String)>,
}

impl Spawned<'_> {
    /// [`std::process::Child::wait`], then grade the run.
    pub fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let status = self.child.wait()?;
        self.reaped(status);
        Ok(status)
    }

    /// [`std::process::Child::try_wait`], then grade the run once it has exited.
    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        if let Some(st) = status {
            self.reaped(st);
        }
        Ok(status)
    }

    /// [`Spawned::wait`] with the (discarded, so empty) output.
    pub fn wait_with_output(mut self) -> std::io::Result<std::process::Output> {
        let status = self.wait()?;
        Ok(std::process::Output {
            status,
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
    }

    /// Grade the run once, by its exit.
    fn reaped(&mut self, status: std::process::ExitStatus) {
        self.rig.absorb_product_config_writes();
        let envs: Vec<(&str, &str)> = self
            .envs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.rig.oracle_settle(self.case.take(), status, &[], &envs);
    }
}

impl std::ops::Deref for Spawned<'_> {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for Spawned<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for Spawned<'_> {
    /// A child dropped before anyone reaped it was never graded: say so.
    fn drop(&mut self) {
        if self.case.is_some() && !std::thread::panicking() {
            crate::common::verify::log(
                "SKIP",
                &self.rig.name,
                "a spawned child dropped before it was reaped",
            );
        }
    }
}
