//! Multi-export execution by spawning one `rivet` child process per export (`--parallel-export-processes`).

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;

use crate::config::ExportConfig;
use crate::error::Result;
use crate::state::StateStore;

use super::ipc::{ChildEvent, ENV_IPC_EVENTS};
use super::parent_ui::{ChildWaitStatus, UiMessage, sanitize_terminal};

/// Decode one stdout line from a CHILD PROCESS and adopt the process-local
/// counters it carries into this process's totals.
///
/// This is the **only** place a `ChildEvent` crosses a process boundary, and
/// therefore the only place its counters are provably FOREIGN: the child's
/// `RetryLayer` incremented its own `TRANSIENT_RETRIES` atomic, which this
/// process never saw, so folding it in is what makes the parent's "dest
/// retries" line cover the whole run instead of just the parent's own probes.
///
/// The fold used to live in `parent_ui::Renderer::handle_event` — but the
/// renderer is also fed by the SAME-process channel (`ipc::emit_event`, which
/// every `--parallel-exports` / `--pool` / plain sequential run installs), so
/// it re-added this process's own counter to itself: a run with 3 real
/// retries reported 6, and each further export doubled the total again
/// (bughunt 2026-08-14, finding #4). Keeping the fold here makes the
/// invariant structural — a counter is folded only after arriving over a
/// pipe from another process.
fn adopt_child_event(line: &str) -> serde_json::Result<ChildEvent> {
    let ev: ChildEvent = serde_json::from_str(line)?;
    if let ChildEvent::Finished { dest_retries, .. } = &ev {
        crate::destination::add_transient_retries(*dest_retries);
    }
    Ok(ev)
}

/// Re-invoke this binary once per export. Children do not inherit parallel flags, so there is no recursion.
///
/// Each child has `RIVET_IPC_EVENTS=1` set in its environment and runs with
/// stdout piped: it emits one JSON line per significant event
/// ([`ChildEvent`]) on stdout instead of drawing its own progress bar
/// or per-export summary.  The parent reads those events in dedicated reader
/// threads and forwards them through an `mpsc` channel to a single UI thread
/// that owns an `indicatif::MultiProgress`, drawing one card per export.
///
/// Returns `(Result, child_failures, stderr_dump)` so the caller can build
/// an aggregate, then surface any captured child-process stderr below the
/// run summary.  `child_failures` maps export name → error message for
/// children that did not exit cleanly; on success this map is empty.
/// `stderr_dump` is a pre-rendered block (already terminated by `\n`) that
/// the caller prints verbatim on stderr; empty when no child wrote
/// anything to its captured stderr.
#[allow(clippy::too_many_arguments)] // forwarding parent's flag set to children
pub(super) fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    reconcile: bool,
    resume: bool,
    force: bool,
    params: Option<&std::collections::HashMap<String, String>>,
    // Wave-wide max export-name width so the card table aligns across the
    // cost-gate's separate safe/lone batches, not just within one batch.
    name_floor: usize,
) -> (Result<()>, HashMap<String, String>, String) {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            return (
                Err(anyhow::anyhow!(
                    "failed to resolve rivet executable for child processes: {:#}",
                    e
                )),
                HashMap::new(),
                String::new(),
            );
        }
    };

    let config_arg = Path::new(config_path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(config_path));

    // Apply schema migrations once in the parent before spawning children.
    if let Err(e) = StateStore::open(config_path) {
        return (
            Err(anyhow::anyhow!(
                "failed to open / migrate state DB before spawning children: {:#}",
                e
            )),
            HashMap::new(),
            String::new(),
        );
    }

    log::info!(
        "running {} exports as separate rivet processes (each child: single `--export`; SQLite state WAL allows concurrent writers; IPC card UI on)",
        exports.len()
    );

    let (tx, rx) = mpsc::channel::<UiMessage>();

    // #153 heartbeat: silence between spawn and the first child event is
    // indistinguishable from a hang (the field: ~2.5 min blank terminal). Emit it
    // BEFORE the UI thread starts drawing — a raw eprintln AFTER the renderer is
    // live races its cursor and corrupts the card table in a TTY (roast
    // 2026-08-09). Printed here, it scrolls above the cards the renderer then
    // owns. Uses the intended count (exports.len()); per-child spawn failures
    // surface as their own ✗ cards.
    eprintln!(
        "  … starting {} export process(es); waiting for the first child event",
        exports.len()
    );

    // `name_floor` (wave-wide max, from the caller) seeds the card table's name
    // column so it aligns from the first redraw and across batches.
    let n_cards = exports.len();
    let ui_handle = std::thread::Builder::new()
        .name("rivet-ipc-ui".into())
        .spawn(move || super::parent_ui::run_ui(rx, name_floor, n_cards))
        .ok();

    const CHILD_STDERR_LINE_CAP: usize = 5_000;
    // One mutex per export — parallel child threads never contend with each other.
    type StderrBuf = std::sync::Arc<std::sync::Mutex<(Vec<String>, usize)>>;
    let stderr_bufs: std::sync::Arc<HashMap<String, StderrBuf>> = std::sync::Arc::new(
        exports
            .iter()
            .map(|e| {
                (
                    e.name.clone(),
                    std::sync::Arc::new(std::sync::Mutex::new((Vec::new(), 0usize))),
                )
            })
            .collect(),
    );

    let mut children: Vec<(String, std::process::Child)> = Vec::with_capacity(exports.len());
    let mut reader_handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(exports.len());
    let mut spawn_failures: HashMap<String, String> = HashMap::new();
    // Reap children on a targeted SIGTERM/SIGINT to the parent so they don't
    // orphan and keep holding source connections (OPT-6).
    child_reaper::install_once();
    for export in exports {
        let mut cmd = Command::new(&exe);
        cmd.arg("run")
            .arg("--config")
            .arg(&config_arg)
            .arg("--export")
            .arg(export.name.as_str());
        if validate {
            cmd.arg("--validate");
        }
        if reconcile {
            cmd.arg("--reconcile");
        }
        if resume {
            cmd.arg("--resume");
        }
        if force {
            cmd.arg("--force");
        }
        if let Some(p) = params {
            for (k, v) in p {
                cmd.arg("--param").arg(format!("{k}={v}"));
            }
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env(ENV_IPC_EVENTS, "1")
            // …and the claim that goes with it: THIS parent will emit the
            // run-over-run throughput self-check for the child's exports, so the
            // child must not (its stderr is captured to a file nobody reads).
            // A separate, internal variable because `RIVET_IPC_EVENTS` is a
            // documented user-facing switch — keying the deferral on it alone
            // silenced the check in any environment that set it (see
            // `run::ENV_PARENT_SELF_CHECK`).
            .env(super::run::ENV_PARENT_SELF_CHECK, "1");
        // Declare sibling concurrency to the child: it runs single-export in
        // its own process, so MULTI_EXPORT_CONCURRENT reads false there and
        // run_diagnosis would print the confident solo harm attribution while
        // N sibling children share the same server-global counter window —
        // the runner-bypass shape of the pool hedge fix (bughunt 2026-08-13).
        // Only when this batch actually has siblings: a lone child (a heavy
        // wave batch of one) keeps the truthful solo attribution.
        if exports.len() > 1 {
            cmd.env(super::run::ENV_CONCURRENT_SIBLINGS, "1");
        }
        log::debug!("spawning child for export '{}': {:?}", export.name, cmd);
        match cmd.spawn() {
            Ok(mut child) => {
                child_reaper::register(child.id());
                if let Some(stdout) = child.stdout.take() {
                    let tx = tx.clone();
                    let export_name = export.name.clone();
                    let h = std::thread::Builder::new()
                        .name(format!("rivet-ipc-rx-{}", export.name))
                        .spawn(move || {
                            let reader = BufReader::new(stdout);
                            for line in reader.lines() {
                                let line = match line {
                                    Ok(l) => l,
                                    Err(e) => {
                                        log::debug!(
                                            "ipc: child '{}' stdout read error: {:#}",
                                            export_name,
                                            e
                                        );
                                        break;
                                    }
                                };
                                let trimmed = line.trim();
                                if trimmed.is_empty() {
                                    continue;
                                }
                                match adopt_child_event(trimmed) {
                                    Ok(ev) => {
                                        let _ = tx.send(UiMessage::Event(ev));
                                    }
                                    Err(e) => {
                                        log::debug!(
                                            "ipc: child '{}' emitted unparsable line: {} ({:#})",
                                            export_name,
                                            trimmed,
                                            e
                                        );
                                    }
                                }
                            }
                        })
                        .ok();
                    if let Some(h) = h {
                        reader_handles.push(h);
                    }
                }
                if let Some(stderr) = child.stderr.take() {
                    let export_name = export.name.clone();
                    let buf = std::sync::Arc::clone(
                        stderr_bufs.get(&export_name).expect("buf pre-allocated"),
                    );
                    let h = std::thread::Builder::new()
                        .name(format!("rivet-ipc-err-{}", export.name))
                        .spawn(move || {
                            let reader = BufReader::new(stderr);
                            for line in reader.lines() {
                                let line = match line {
                                    Ok(l) => l,
                                    Err(_) => break,
                                };
                                // Captured child stderr is later re-emitted to
                                // the operator's terminal verbatim, so strip any
                                // terminal-control bytes a malicious source DB
                                // echoed into the child's error output (CWE-150).
                                let line = sanitize_terminal(&line);
                                let mut guard = match buf.lock() {
                                    Ok(g) => g,
                                    Err(p) => p.into_inner(),
                                };
                                if guard.0.len() >= CHILD_STDERR_LINE_CAP {
                                    guard.1 += 1;
                                } else {
                                    guard.0.push(line);
                                }
                            }
                        })
                        .ok();
                    if let Some(h) = h {
                        reader_handles.push(h);
                    }
                }
                children.push((export.name.clone(), child));
            }
            Err(e) => {
                spawn_failures.insert(export.name.clone(), format!("spawn failed: {e:#}"));
            }
        }
    }

    let mut failures = Vec::new();
    let mut wait_failures: HashMap<String, String> = HashMap::new();
    // Numeric exit codes of failed children, so the parent can re-derive the
    // SAME process exit class instead of collapsing a child's data-integrity (3)
    // / schema-drift (4) / retryable (2) into a misleading generic 1.
    let mut child_exit_codes: Vec<i32> = Vec::new();

    type WaitOutcome = (String, std::io::Result<std::process::ExitStatus>);
    let mut reaper_handles: Vec<std::thread::JoinHandle<WaitOutcome>> =
        Vec::with_capacity(children.len());
    for (name, mut child) in children {
        let pid = child.id();
        let handle = std::thread::Builder::new()
            .name(format!("rivet-reap-{}", name))
            .spawn(move || {
                let status = child.wait();
                // Reaped — clear before the PID can be reused, so a later signal
                // never targets an unrelated process.
                child_reaper::deregister(pid);
                (name, status)
            });
        match handle {
            Ok(h) => reaper_handles.push(h),
            Err(e) => {
                log::debug!("ipc: failed to spawn reaper thread: {:#}", e);
            }
        }
    }
    for h in reaper_handles {
        let (name, status) = match h.join() {
            Ok(pair) => pair,
            Err(payload) => std::panic::resume_unwind(payload),
        };
        let status = match status {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("wait failed: {e:#}");
                failures.push(format!("export '{name}': {msg}"));
                wait_failures.insert(name.clone(), msg.clone());
                let _ = tx.send(UiMessage::ChildClosed {
                    export_name: name,
                    wait_status: ChildWaitStatus::Failed(msg),
                });
                continue;
            }
        };
        if !status.success() {
            if let Some(c) = status.code() {
                child_exit_codes.push(c);
            }
            let code = status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string());
            let msg = format!("exited with status {code}");
            failures.push(format!("export '{name}' {msg}"));
            wait_failures.insert(name.clone(), msg.clone());
            let _ = tx.send(UiMessage::ChildClosed {
                export_name: name,
                wait_status: ChildWaitStatus::Failed(msg),
            });
        } else {
            let _ = tx.send(UiMessage::ChildClosed {
                export_name: name,
                wait_status: ChildWaitStatus::Success,
            });
        }
    }

    drop(tx);
    for h in reader_handles {
        let _ = h.join();
    }
    if let Some(h) = ui_handle {
        let _ = h.join();
    }

    let mut stderr_snapshot: HashMap<String, Vec<String>> = HashMap::new();
    let mut truncated_snapshot: HashMap<String, usize> = HashMap::new();
    for (name, buf) in stderr_bufs.as_ref() {
        let guard = match buf.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if !guard.0.is_empty() {
            stderr_snapshot.insert(name.clone(), guard.0.clone());
        }
        if guard.1 > 0 {
            truncated_snapshot.insert(name.clone(), guard.1);
        }
    }
    let stderr_dump = render_child_stderr(exports, &stderr_snapshot, &truncated_snapshot);

    let mut all_failures = spawn_failures;
    all_failures.extend(wait_failures);
    for (name, msg) in &all_failures {
        if !failures.iter().any(|f| f.contains(name)) {
            failures.push(format!("export '{name}': {msg}"));
        }
    }

    // #153: a common-mode startup failure — surface ONE representative excerpt
    // NOW (before the caller's end-of-batch full dump), so the operator sees the
    // one message that explains the flood instead of scanning 70 identical cards.
    // Representative message per failed child = its last stderr line (the error),
    // else the wait/spawn reason. `any_success` = at least one child produced no
    // failure record.
    let any_success = exports.iter().any(|e| !all_failures.contains_key(&e.name));
    let failed_msgs: Vec<(String, String)> = all_failures
        .iter()
        .map(|(name, reason)| {
            let msg = stderr_snapshot
                .get(name)
                .and_then(|lines| lines.iter().rev().find(|l| !l.trim().is_empty()))
                .cloned()
                .unwrap_or_else(|| reason.clone());
            (name.clone(), msg)
        })
        .collect();
    if let Some(banner) = representative_error(&failed_msgs, any_success, 3) {
        eprint!("{banner}");
    }

    let result = aggregate_child_result(&failures, &child_exit_codes);
    (result, all_failures, stderr_dump)
}

/// #153: when K+ children fail with the SAME error class before any success,
/// surface ONE representative excerpt instead of a silent wait then a flood of
/// identical ✗ cards (the field: 70+ children failed on absent dest creds; the
/// operator saw a blank terminal then 70 cards, none of which explained why).
///
/// Pure so the dedup/threshold is unit-tested. `failed` is `(name, last_stderr
/// line)` per failed child; `any_success` suppresses the banner (a mixed batch
/// is not a common-mode failure). Class key = the message with digits/hex/paths
/// blanked, so `token for host-1` and `token for host-2` collapse to one class.
/// Returns `None` below the threshold or on a mixed/empty batch.
/// The class KEY of one error message — variable tokens masked so per-child ids
/// collapse to one common-mode class. A pure DIGIT run → `#` (host-1 / host-10
/// are one class); a run mixing letters AND digits → `#` whole (a hex id, uuid,
/// or random staging suffix like `aB3xQ9k` — digit-masking alone left `aB#xQ#k`,
/// still per-child distinct, so the banner never fired: roast 2026-08-09). A
/// pure-letter run (a real word) is KEPT. Whitespace normalized. Pure +
/// directly unit-tested with exact expected strings.
pub(super) fn error_class(msg: &str) -> String {
    let mask_token = |tok: &str| -> String {
        // Each token is a run of `is_ascii_alphanumeric()` chars, so every char is a digit or an
        // ASCII letter; `has_alpha || all-digits` is therefore always true and the mask condition
        // reduces to just `has_digit` (mask any token that carries a digit — an id/hash/token).
        if tok.chars().any(|c| c.is_ascii_digit()) {
            "#".to_string()
        } else {
            tok.to_string()
        }
    };
    let mut out = String::with_capacity(msg.len());
    let mut run = String::new();
    for c in msg.chars() {
        if c.is_ascii_alphanumeric() {
            run.push(c);
        } else {
            if !run.is_empty() {
                out.push_str(&mask_token(&run));
                run.clear();
            }
            out.push(c);
        }
    }
    if !run.is_empty() {
        out.push_str(&mask_token(&run));
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) fn representative_error(
    failed: &[(String, String)],
    any_success: bool,
    threshold: usize,
) -> Option<String> {
    if any_success || failed.len() < threshold {
        return None;
    }
    let mut counts: std::collections::HashMap<String, (usize, String)> =
        std::collections::HashMap::new();
    for (_, msg) in failed {
        let m = msg.trim();
        if m.is_empty() {
            continue;
        }
        let e = counts.entry(error_class(m)).or_insert((0, m.to_string()));
        e.0 += 1;
    }
    let (n, sample) = counts.into_values().max_by_key(|(n, _)| *n)?;
    if n < threshold {
        return None;
    }
    let more = failed.len().saturating_sub(n);
    let tail = if more > 0 {
        format!(" (and {more} more child failure(s), not all this class)")
    } else {
        format!(" (all {n} children failed alike)")
    };
    Some(format!(
        "\n  ⚠ {n} children failed with the same error before any succeeded — \
         representative:\n      {sample}{tail}\n      (full per-child stderr follows at batch end)\n"
    ))
}

/// Build the parent's final result from the children's outcomes. When any child
/// exited with a CLASSIFIED non-generic code, the aggregate error carries a
/// [`crate::error::PreclassifiedExit`] so `classify_exit` re-derives that SAME
/// process exit class — otherwise a child's data-integrity (3) / schema-drift
/// (4) / retryable (2) would be stringified and the parent would exit a
/// misleading generic 1.
fn aggregate_child_result(failures: &[String], child_exit_codes: &[i32]) -> anyhow::Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    let msg = failures.join("; ");
    match worst_exit_code(child_exit_codes) {
        Some(code) => Err(anyhow::Error::from(crate::error::PreclassifiedExit(code)).context(msg)),
        None => Err(anyhow::anyhow!("{}", msg)),
    }
}

/// The most "stop-worthy" exit code among failed children: data-integrity (3)
/// outranks schema-drift (4), which outranks retryable (2), which outranks
/// generic / anything else (1) — matching the representative-failure ranking the
/// in-process multi-export path uses. `None` when no child reported a code.
fn worst_exit_code(codes: &[i32]) -> Option<i32> {
    let rank = |c: i32| match c {
        3 => 3, // data-integrity — STOP, possibly-wrong data
        4 => 2, // schema-drift — needs human review
        2 => 1, // retryable — safe to retry
        _ => 0, // generic / signal
    };
    codes.iter().copied().max_by_key(|&c| rank(c))
}

/// Best-effort reaping of subprocess-export children when the parent receives a
/// targeted SIGTERM/SIGINT.
///
/// Children are spawned in the parent's process group, so a *terminal* Ctrl-C
/// (SIGINT to the foreground group) already reaches them. But a **targeted**
/// `kill <parent_pid>` — systemd stop, a k8s pod termination, a scheduler — hits
/// only the parent; without this the children orphan and keep holding source
/// connections, the exact fragility rivet exists to protect (OPT-6).
///
/// Signal-safe by construction: the handler does only async-signal-safe work —
/// atomic loads from a fixed-size registry, `kill(2)`, then restore-default +
/// `raise(2)`. No allocation, no locks in the handler.
#[cfg(unix)]
mod child_reaper {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    /// Far above any real export fan-out; the registry is fixed so the handler
    /// touches no allocator. An overflow child is simply untracked (never UB).
    const CAP: usize = 512;
    static SLOTS: [AtomicI32; CAP] = [const { AtomicI32::new(0) }; CAP];
    static INSTALLED: AtomicBool = AtomicBool::new(false);

    /// Record a live child PID so the handler can signal it.
    pub(super) fn register(pid: u32) {
        let pid = pid as i32;
        for slot in &SLOTS {
            if slot
                .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
        log::debug!("child_reaper: registry full ({CAP}); pid {pid} not tracked");
    }

    /// Clear a PID once the parent has `wait`ed it — so a future PID reuse can
    /// never be signalled by a late crash.
    pub(super) fn deregister(pid: u32) {
        let pid = pid as i32;
        for slot in &SLOTS {
            let _ = slot.compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    extern "C" fn handle(sig: libc::c_int) {
        for slot in &SLOTS {
            let pid = slot.load(Ordering::Acquire);
            if pid != 0 {
                // SAFETY: `kill` is async-signal-safe. A reaped PID is cleared
                // before reuse (see `deregister`); a zombie kill is a harmless
                // no-op.
                unsafe { libc::kill(pid, libc::SIGTERM) };
            }
        }
        // Restore the default disposition and re-raise so the parent still dies
        // *with* the signal (correct exit status for the scheduler that sent it).
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
    }

    /// Install the SIGTERM/SIGINT handler exactly once for the parent process.
    pub(super) fn install_once() {
        if INSTALLED.swap(true, Ordering::SeqCst) {
            return;
        }
        // SAFETY: `handle` is async-signal-safe (see its body). No SA_RESTART, so
        // a blocking `wait` in a reaper thread is interrupted by the signal.
        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            // Cast through the fn-pointer type (not the fn item) to satisfy
            // `clippy::fn_to_numeric_cast` under `-D warnings`.
            act.sa_sigaction = handle as extern "C" fn(libc::c_int) as libc::sighandler_t;
            libc::sigemptyset(&mut act.sa_mask);
            act.sa_flags = 0;
            libc::sigaction(libc::SIGTERM, &act, std::ptr::null_mut());
            libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
        }
    }
}

#[cfg(not(unix))]
mod child_reaper {
    pub(super) fn register(_pid: u32) {}
    pub(super) fn deregister(_pid: u32) {}
    pub(super) fn install_once() {}
}

#[cfg(test)]
mod foreign_counter_tests {
    use super::*;

    /// The process boundary MUST still fold the child's retry counter — the
    /// half of finding #4 that the fix could silently break by simply
    /// deleting the fold from the renderer.
    ///
    /// The stdout reader thread needs a real spawned `rivet` child, so what is
    /// pinned here is the decode-and-adopt seam the reader calls
    /// ([`adopt_child_event`], its only call site) fed the exact wire bytes a
    /// child writes — `serde_json::to_string(&ChildEvent)` is literally what
    /// `ipc::emit` puts on stdout.
    ///
    /// TWO children, with different counts: the fold accumulates, so one
    /// event cannot tell "added the child's count" from "assigned it".
    ///
    /// Mutant: drop the `add_transient_retries` call from
    /// `adopt_child_event` → the counter does not move and `grew_by` is 0.
    #[test]
    fn a_child_processs_finished_line_folds_its_foreign_retry_counter() {
        let _serial = super::super::parent_ui::retry_counter_test_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());

        let line = |name: &str, dest_retries: u64| {
            serde_json::to_string(&ChildEvent::Finished {
                export_name: name.into(),
                run_id: format!("{name}_20260814T000000"),
                status: "success".into(),
                total_rows: 10,
                files_produced: 1,
                bytes_written: 100,
                duration_ms: 5,
                peak_rss_mb: 7,
                error_message: None,
                dest_retries,
            })
            .unwrap()
        };

        let before = crate::destination::transient_retries_total();
        for (name, n) in [("orders", 3u64), ("events", 4u64)] {
            let ev = adopt_child_event(&line(name, n)).expect("child wire format must parse");
            assert!(matches!(ev, ChildEvent::Finished { .. }));
        }
        let grew_by = crate::destination::transient_retries_total() - before;
        // `>=` not `==`: `destination::cloud`'s interceptor test is outside
        // this module's lock and adds 2. It cannot reach 7, so the mutant
        // (growth 0..2) still goes RED.
        assert!(
            grew_by >= 7,
            "both children's foreign counters must be folded (grew by {grew_by}, want >= 7)"
        );
    }

    /// A line the child could not have written is rejected BEFORE the fold
    /// (the `?` in `adopt_child_event`), so the reader keeps going and the
    /// counter is untouched. Asserted on the return value rather than the
    /// global counter — `destination::cloud`'s interceptor test runs outside
    /// this lock, so an equality on a process-wide counter would flake.
    #[test]
    fn an_unparsable_line_is_rejected_before_the_fold() {
        assert!(adopt_child_event("{not json").is_err());
        assert!(adopt_child_event(r#"{"type":"nope"}"#).is_err());
    }
}

#[cfg(test)]
mod exit_propagation_tests {
    use super::{aggregate_child_result, worst_exit_code};
    use crate::error::{ExitClass, classify_exit};

    #[test]
    fn worst_code_prefers_data_integrity_over_drift_and_generic() {
        // Data-integrity (3) is the scariest even though 4 is numerically larger
        // and 1 came first — a naive max(code) or first-wins would pick wrong.
        assert_eq!(worst_exit_code(&[1, 4, 3, 2]), Some(3));
        assert_eq!(worst_exit_code(&[1, 4, 2]), Some(4));
        assert_eq!(worst_exit_code(&[1, 2]), Some(2));
        assert_eq!(worst_exit_code(&[1, 1]), Some(1));
        assert_eq!(worst_exit_code(&[]), None);
    }

    #[test]
    fn child_data_integrity_exit_3_is_not_downgraded_to_1() {
        // The regression: a child that exited 3 (data-integrity) must make the
        // PARENT classify to 3, not collapse to a generic 1.
        let failures = vec!["export 'a' exited with status 3".to_string()];
        let err = aggregate_child_result(&failures, &[3]).unwrap_err();
        assert_eq!(
            classify_exit(&err),
            ExitClass::DataIntegrity.code(),
            "a child's data-integrity exit must survive to the parent's exit code"
        );
    }

    #[test]
    fn child_schema_drift_exit_4_survives() {
        let failures = vec!["export 'a' exited with status 4".to_string()];
        let err = aggregate_child_result(&failures, &[4]).unwrap_err();
        assert_eq!(classify_exit(&err), ExitClass::SchemaDrift.code());
    }

    #[test]
    fn generic_only_children_stay_generic_1() {
        let failures = vec!["export 'a' exited with status 1".to_string()];
        let err = aggregate_child_result(&failures, &[1]).unwrap_err();
        assert_eq!(classify_exit(&err), ExitClass::Generic.code());
    }

    #[test]
    fn no_failures_is_ok() {
        assert!(aggregate_child_result(&[], &[]).is_ok());
    }
}

fn render_child_stderr(
    exports: &[&ExportConfig],
    buffers: &HashMap<String, Vec<String>>,
    truncated: &HashMap<String, usize>,
) -> String {
    let any = exports
        .iter()
        .any(|e| buffers.get(&e.name).is_some_and(|v| !v.is_empty()));
    if !any {
        return String::new();
    }
    let mut out = String::new();
    out.push('\n');
    out.push_str("  child stderr (captured to keep the live card stack stable):\n");
    for export in exports {
        let Some(lines) = buffers.get(&export.name) else {
            continue;
        };
        if lines.is_empty() {
            continue;
        }
        out.push_str(&format!("  ── {} ──\n", export.name));
        for line in lines {
            out.push_str("    | ");
            out.push_str(line);
            out.push('\n');
        }
        if let Some(extra) = truncated.get(&export.name) {
            out.push_str(&format!(
                "    | … (truncated, {} more line(s) dropped)\n",
                extra
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_export(name: &str) -> crate::config::ExportConfig {
        let yaml = format!(
            "name: {name}\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n"
        );
        serde_yaml_ng::from_str(&yaml).expect("parse test ExportConfig")
    }

    #[test]
    fn empty_buffers_returns_empty_string() {
        let exp = make_export("orders");
        let exports = vec![&exp];
        let out = render_child_stderr(&exports, &HashMap::new(), &HashMap::new());
        assert!(out.is_empty(), "no stderr → empty output, got: {out:?}");
    }

    #[test]
    fn single_export_with_stderr_lines_rendered() {
        let exp = make_export("orders");
        let exports = vec![&exp];
        let mut buffers = HashMap::new();
        buffers.insert(
            "orders".to_string(),
            vec!["INFO starting".to_string(), "WARN slow query".to_string()],
        );
        let out = render_child_stderr(&exports, &buffers, &HashMap::new());
        assert!(out.contains("── orders ──"), "should have header: {out}");
        assert!(out.contains("INFO starting"), "should have line 1: {out}");
        assert!(out.contains("WARN slow query"), "should have line 2: {out}");
        assert!(out.contains("| "), "lines prefixed with |: {out}");
    }

    #[test]
    fn truncated_count_appended() {
        let exp = make_export("payments");
        let exports = vec![&exp];
        let mut buffers = HashMap::new();
        buffers.insert("payments".to_string(), vec!["some line".to_string()]);
        let mut truncated = HashMap::new();
        truncated.insert("payments".to_string(), 42usize);
        let out = render_child_stderr(&exports, &buffers, &truncated);
        assert!(
            out.contains("42 more line(s) dropped"),
            "truncation note: {out}"
        );
    }

    #[test]
    fn export_not_in_buffers_is_skipped() {
        let exp = make_export("users");
        let exports = vec![&exp];
        // buffers has a *different* export
        let mut buffers = HashMap::new();
        buffers.insert("other".to_string(), vec!["line".to_string()]);
        let out = render_child_stderr(&exports, &buffers, &HashMap::new());
        // "users" has no entry → no output (other is not in exports list)
        assert!(out.is_empty(), "unrelated export not rendered: {out:?}");
    }

    #[test]
    fn multiple_exports_ordering_matches_exports_slice() {
        let exp_a = make_export("alpha");
        let exp_b = make_export("beta");
        let exports = vec![&exp_a, &exp_b];
        let mut buffers = HashMap::new();
        buffers.insert("alpha".to_string(), vec!["line_a".to_string()]);
        buffers.insert("beta".to_string(), vec!["line_b".to_string()]);
        let out = render_child_stderr(&exports, &buffers, &HashMap::new());
        let pos_a = out.find("alpha").expect("alpha in output");
        let pos_b = out.find("beta").expect("beta in output");
        assert!(pos_a < pos_b, "alpha rendered before beta");
    }

    #[test]
    fn export_with_empty_lines_vec_not_rendered() {
        let exp = make_export("events");
        let exports = vec![&exp];
        let mut buffers = HashMap::new();
        // Export has an entry but it's empty.
        buffers.insert("events".to_string(), vec![]);
        let out = render_child_stderr(&exports, &buffers, &HashMap::new());
        assert!(out.is_empty(), "empty lines vec → no output: {out:?}");
    }

    /// SEC V9 (CWE-150): a child stderr line carrying ANSI/OSC escape bytes is
    /// sanitised by the capture thread (`sanitize_terminal`) before it lands in
    /// the buffer, so the rendered block re-emitted to the operator's terminal
    /// holds no raw terminal-control bytes. This mirrors the reader-thread
    /// transform without spawning a real child.
    #[test]
    fn captured_child_stderr_escapes_stripped_before_render() {
        // Scan decoded chars, not raw bytes: a byte scan would false-flag the
        // UTF-8 continuation bytes (0x80..=0xBF) of the renderer's box-drawing
        // glyph `──` (U+2500). Mirrors sanitize_terminal's char-based contract.
        let dangerous = |s: &str| -> Vec<char> {
            s.chars()
                .filter(|&c| {
                    let cp = c as u32;
                    (cp <= 0x1f && c != '\t' && c != '\n')
                        || cp == 0x7f
                        || (0x80..=0x9f).contains(&cp)
                })
                .collect()
        };
        let exp = make_export("orders");
        let exports = vec![&exp];
        let raw = "ERROR \u{1b}]0;pwned\u{07}\u{1b}[2Jboom";
        let mut buffers = HashMap::new();
        // Capture thread applies `sanitize_terminal` to each line before push.
        buffers.insert("orders".to_string(), vec![sanitize_terminal(raw)]);
        let out = render_child_stderr(&exports, &buffers, &HashMap::new());
        assert!(
            dangerous(&out).is_empty(),
            "child stderr render leaked control bytes: {out:?}"
        );
        assert!(out.contains("pwned") && out.contains("boom"));
    }
}

#[cfg(test)]
mod representative_tests {
    use super::{error_class, representative_error};

    /// #197 (roast 2026-08-09): direct exact-string coverage of error_class, the
    /// pure class-key masker. Each assertion pins a mutant the banner-level tests
    /// couldn\'t (over-masking still collapses to one class): pure-letter words
    /// KEPT (399 `&&`→`||` would mask them), the trailing run flushed (418
    /// `delete !` would drop the last token), mid-token runs flushed at
    /// separators (411 `delete !` would fuse tokens).
    #[test]
    fn error_class_masks_variable_tokens_exactly() {
        // digit run → #; host-1 / host-10 are one class.
        assert_eq!(error_class("token for host-42"), "token for host-#");
        // mixed alnum (random suffix / hex) → whole run to #.
        assert_eq!(
            error_class("dir /tmp/rivet-stage-aB3xQ9k: denied"),
            "dir /tmp/rivet-stage-#: denied"
        );
        // pure-letter words are KEPT (399 `&&`→`||` masks them → RED).
        assert_eq!(error_class("plain words only"), "plain words only");
        // trailing alnum run (no trailing separator) is flushed (418 `!` → RED,
        // it would drop "here").
        assert_eq!(error_class("ends here"), "ends here");
        // mid runs flush at each separator (411 `!` → RED, it would fuse to "ab").
        assert_eq!(error_class("a b"), "a b");
        // whitespace normalized.
        assert_eq!(error_class("two   spaces"), "two spaces");
    }

    #[test]
    fn representative_collapses_a_common_mode_failure() {
        // 70 children, same class (creds absent for different hosts) → ONE banner.
        let failed: Vec<(String, String)> = (0..70)
            .map(|i| {
                (
                    format!("t{i}"),
                    format!("Error: could not obtain token for host-{i}"),
                )
            })
            .collect();
        let banner = representative_error(&failed, false, 3).expect("common-mode → banner");
        assert!(banner.contains("70 children failed"), "{banner}");
        assert!(banner.contains("could not obtain token"), "{banner}");
    }

    #[test]
    fn representative_suppressed_on_mixed_or_below_threshold() {
        let two: Vec<(String, String)> = (0..2)
            .map(|i| (format!("t{i}"), "Error: x".into()))
            .collect();
        assert!(
            representative_error(&two, false, 3).is_none(),
            "below threshold"
        );
        let many: Vec<(String, String)> = (0..10)
            .map(|i| (format!("t{i}"), "Error: x".into()))
            .collect();
        assert!(
            representative_error(&many, true, 3).is_none(),
            "a batch with a success is not a common-mode failure"
        );
    }

    /// #153 D2 (roast 2026-08-09): a per-child RANDOM ALPHANUMERIC token (a
    /// staging-dir suffix, a hex id) must not defeat the common-mode banner —
    /// the class key must collapse mixed alnum runs whole, not just digit runs.
    /// RED against the digit-only class_of (which left aB#xQ#k per-child distinct).
    #[test]
    fn representative_collapses_random_alphanumeric_suffixes() {
        let failed: Vec<(String, String)> = (0..40)
            .map(|i| {
                // e.g. aB3xQ9k — LETTERS differ per child (not just digits), so
                // digit-masking alone leaves distinct classes; only whole-run
                // masking collapses them.
                let a = (b'a' + (i % 20) as u8) as char;
                let b = (b'A' + ((i * 3) % 20) as u8) as char;
                let suffix = format!("{a}{i}x{b}{}k", i * 7 + 3);
                (
                    format!("child{i}"),
                    format!("Error: could not create staging dir /tmp/rivet-stage-{suffix}: Permission denied (os error 13)"),
                )
            })
            .collect();
        let banner = representative_error(&failed, false, 3)
            .expect("random suffixes must still collapse to ONE common-mode class");
        assert!(banner.contains("40 children failed"), "{banner}");
    }

    #[test]
    fn representative_picks_the_dominant_class_when_mixed() {
        let mut failed: Vec<(String, String)> = (0..8)
            .map(|i| (format!("a{i}"), format!("Error: token for host-{i}")))
            .collect();
        failed.push(("b".into(), "Error: disk full".into()));
        let banner = representative_error(&failed, false, 3).unwrap();
        assert!(
            banner.contains("8 children failed"),
            "dominant class: {banner}"
        );
        assert!(
            banner.contains("more child failure(s)"),
            "names the tail: {banner}"
        );
    }
}
