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
use super::parent_ui::{ChildWaitStatus, UiMessage};

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
pub(super) fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
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

    let ui_handle = std::thread::Builder::new()
        .name("rivet-ipc-ui".into())
        .spawn(move || super::parent_ui::run_ui(rx))
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

    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{}", failures.join("; ")))
    };
    (result, all_failures, stderr_dump)
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
