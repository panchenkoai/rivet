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
        let mut cmd = std::process::Command::new(crate::common::runner::RIVET_BIN);
        cmd.args(argv);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        cmd
    }

    /// Run to completion and collect the output.
    fn invoke(&self, argv: &[String], envs: &[(&str, &str)]) -> std::process::Output {
        self.invoke_command(argv, envs)
            .output()
            .expect("spawn rivet binary")
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
        let mut args: Vec<&str> = vec!["apply", plan.to_str().expect("plan path must be utf-8")];
        args.extend_from_slice(extra);
        crate::common::runner::run_rivet_env(&args, envs)
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

    /// Spawn `rivet run` and hand back the LIVE child, output discarded.
    ///
    /// For tests that must act on a running process — signal it, inspect its
    /// children, watch the staged `.tmp` appear — rather than wait for an exit
    /// status. `run_args_env` blocks until completion and so cannot express them.
    /// The caller owns the `Child` and must reap it.
    pub fn spawn_args_env(&self, extra: &[&str], envs: &[(&str, &str)]) -> std::process::Child {
        self.invoke_command(&self.run_argv(extra), envs)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rivet")
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
}
