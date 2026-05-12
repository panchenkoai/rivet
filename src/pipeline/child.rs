use crate::config::ExportConfig;
use crate::error::Result;
use crate::state::StateStore;

use super::ipc::{ChildEvent, ENV_IPC_EVENTS};
use super::parent_ui::{self, ChildWaitStatus, UiMessage};

/// Re-invoke this binary once per export. Children do not inherit parallel flags, so there is no recursion.
///
/// Each child has `RIVET_IPC_EVENTS=1` set in its environment and runs with
/// stdout piped: it emits one JSON line per significant event
/// ([`super::ipc::ChildEvent`]) on stdout instead of drawing its own progress bar
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
pub(super) fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
) -> (
    Result<()>,
    std::collections::HashMap<String, String>,
    String,
) {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            return (
                Err(anyhow::anyhow!(
                    "failed to resolve rivet executable for child processes: {:#}",
                    e
                )),
                std::collections::HashMap::new(),
                String::new(),
            );
        }
    };

    let config_arg = std::path::Path::new(config_path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(config_path));

    // Apply schema migrations once in the parent before spawning children.
    // Without this, every child would race on `migrate()` and one of them
    // would surface "database is locked" (SQLite serialises writers, but
    // each child opens its own connection and runs the migration BEGIN/
    // COMMIT in parallel).  After this call the DB is at the latest
    // schema version, so each child's `StateStore::open` finds nothing to
    // do and proceeds straight to the read/write workload.
    if let Err(e) = StateStore::open(config_path) {
        return (
            Err(anyhow::anyhow!(
                "failed to open / migrate state DB before spawning children: {:#}",
                e
            )),
            std::collections::HashMap::new(),
            String::new(),
        );
    }

    log::info!(
        "running {} exports as separate rivet processes (each child: single `--export`; SQLite state WAL allows concurrent writers; IPC card UI on)",
        exports.len()
    );

    // Single channel feeds the UI thread.  Each child gets a stdout reader
    // thread that parses NDJSON and forwards events with the export name
    // resolved from the event itself (not from the child handle), so out-of-
    // order events still route correctly.
    let (tx, rx) = mpsc::channel::<UiMessage>();

    let ui_handle = std::thread::Builder::new()
        .name("rivet-ipc-ui".into())
        .spawn(move || parent_ui::run_ui(rx))
        .ok();

    // Per-child stderr buffer, drained after the live UI commits.  Children
    // inherit `env_logger` and would otherwise scribble straight onto our
    // terminal between card redraws — corrupting the cursor-up math for the
    // in-place renderer (`parent_ui::Renderer`).  Capping each child's
    // buffered output prevents a runaway log loop from consuming unbounded
    // memory.
    const CHILD_STDERR_LINE_CAP: usize = 5_000;
    let stderr_buffers: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, Vec<String>>>,
    > = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let stderr_truncated: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, usize>>,
    > = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

    let mut children: Vec<(String, std::process::Child)> = Vec::with_capacity(exports.len());
    let mut reader_handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(exports.len());
    let mut spawn_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
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
        if let Some(p) = params {
            for (k, v) in p {
                cmd.arg("--param").arg(format!("{k}={v}"));
            }
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            // Child stderr is captured (not inherited) so `env_logger`
            // output from the children doesn't interleave with — and
            // corrupt — the in-place card renderer.  Buffered lines are
            // printed below the run summary once the UI has committed.
            .stderr(Stdio::piped())
            .env(ENV_IPC_EVENTS, "1");
        log::debug!("spawning child for export '{}': {:?}", export.name, cmd);
        match cmd.spawn() {
            Ok(mut child) => {
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
                                match serde_json::from_str::<ChildEvent>(trimmed) {
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
                    let buffers = std::sync::Arc::clone(&stderr_buffers);
                    let truncated = std::sync::Arc::clone(&stderr_truncated);
                    let h = std::thread::Builder::new()
                        .name(format!("rivet-ipc-err-{}", export.name))
                        .spawn(move || {
                            let reader = BufReader::new(stderr);
                            for line in reader.lines() {
                                let line = match line {
                                    Ok(l) => l,
                                    Err(_) => break,
                                };
                                let mut guard = match buffers.lock() {
                                    Ok(g) => g,
                                    Err(p) => p.into_inner(),
                                };
                                let entry =
                                    guard.entry(export_name.clone()).or_insert_with(Vec::new);
                                if entry.len() >= CHILD_STDERR_LINE_CAP {
                                    drop(guard);
                                    let mut tg = match truncated.lock() {
                                        Ok(g) => g,
                                        Err(p) => p.into_inner(),
                                    };
                                    *tg.entry(export_name.clone()).or_insert(0) += 1;
                                    continue;
                                }
                                entry.push(line);
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
    let mut wait_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // Reap each child in its own dedicated thread so a child that exits
    // gets `wait()`ed immediately, regardless of which child is at the
    // head of the spawn order.  The previous loop reaped children
    // sequentially, which on a 15-export config left up to 14 finished
    // children sitting around as zombies until the longest-running one
    // (e.g. `events` with 400 chunks) caught up — visible in `htop` /
    // `Activity Monitor` as a long stack of `rivet --export …` rows that
    // had clearly already done their work.
    type WaitOutcome = (String, std::io::Result<std::process::ExitStatus>);
    let mut reaper_handles: Vec<std::thread::JoinHandle<WaitOutcome>> =
        Vec::with_capacity(children.len());
    for (name, mut child) in children {
        let handle = std::thread::Builder::new()
            .name(format!("rivet-reap-{}", name))
            .spawn(move || {
                let status = child.wait();
                (name, status)
            });
        match handle {
            Ok(h) => reaper_handles.push(h),
            Err(e) => {
                // We can't spawn a reap thread; fall back to inline wait
                // so we don't leak the child handle.  The child still
                // gets reaped, just without the parallelism benefit.
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

    // Drop our own sender so the UI thread can observe the channel closing
    // once every reader thread has also exited.
    drop(tx);
    for h in reader_handles {
        let _ = h.join();
    }
    if let Some(h) = ui_handle {
        let _ = h.join();
    }

    // Now that the UI has committed, render any captured child stderr.
    // Caller prints this AFTER the run-summary aggregate so the summary is
    // immediately below the card stack, with verbose logs at the bottom.
    let stderr_snapshot = stderr_buffers.lock().map(|g| g.clone()).unwrap_or_default();
    let truncated_snapshot = stderr_truncated
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    let stderr_dump = render_child_stderr(exports, &stderr_snapshot, &truncated_snapshot);

    let mut all_failures = spawn_failures;
    all_failures.extend(wait_failures);
    for (name, msg) in &all_failures {
        if !failures.iter().any(|f| f.contains(name)) {
            failures.push(format!("export '{name}': {msg}"));
        }
    }

    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{}", failures.join("; ")))
    };
    (result, all_failures, stderr_dump)
}

/// Render captured child-process stderr to a single string, ready to be
/// written to stderr by the caller (after the run-summary aggregate).
/// Returns an empty string when no child wrote anything, so successful
/// quiet runs stay clean.  Lines are emitted with a `  | ` prefix so
/// they're visually distinct from rivet's own UI lines.
fn render_child_stderr(
    exports: &[&ExportConfig],
    buffers: &std::collections::HashMap<String, Vec<String>>,
    truncated: &std::collections::HashMap<String, usize>,
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
