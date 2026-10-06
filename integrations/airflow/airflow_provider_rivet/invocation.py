"""The one place a rivet process is built, run, read and classified; it imports nothing from Airflow."""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import _yaml
from .preflight import Preflight, run_preflight
from .result import ErrorObject, UnitResult, error_from_exit, error_from_line

Emit = Callable[[dict[str, Any]], None]
COMMANDS = ("run", "cdc_run", "apply", "plan", "load", "compact")
DONE_WORD = {"load": "loaded", "compact": "compacted"}
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass
class Request:
    """Everything one rivet task needs; secrets are only ever inside `env`."""

    command: str
    config: str
    env: Mapping[str, str]
    export: Optional[str] = None
    table: Optional[str] = None
    rivet_bin: str = "rivet"
    state_dir: Optional[str] = None
    deployment: str = "auto"
    extra_args: Sequence[str] = ()
    lock_wait: int = 600
    plan_file: Optional[str] = None
    cwd: Optional[str] = None
    stderr_dir: Optional[str] = None
    stream_stderr: bool = False
    label: Sequence[str] = ("dag", "run", "task", "1")
    upstream_markers: Sequence[str] = ()


@dataclass
class Outcome:
    """What a task reports: its units, the representative error and what the binary could not do."""

    command: str
    units: list[UnitResult] = field(default_factory=list)
    error: Optional[ErrorObject] = None
    exit_status: Optional[int] = 0
    signal: Optional[int] = None
    degraded: list[str] = field(default_factory=list)
    stderr_path: Optional[str] = None
    argv: list[list[str]] = field(default_factory=list)
    marker: Optional[str] = None
    rivet_version: Optional[str] = None
    skip_reason: Optional[str] = None

    @property
    def decision(self) -> str:
        """`success`, `retry` (transient budget), `retry_crashed` (its own budget) or `fail`."""
        if self.error is None:
            return "success"
        if self.error.class_ == "crashed":
            return "retry_crashed"
        return "retry" if self.error.retryable else "fail"

    def to_xcom(self) -> dict[str, Any]:
        """The XCom payload: units without `message`, never an environment value."""
        return {
            "command": self.command,
            "decision": self.decision,
            "units": [u.to_xcom() for u in self.units],
            "error": self.error.to_xcom() if self.error else None,
            "degraded": list(self.degraded),
            "exit_status": self.exit_status,
            "signal": self.signal,
            "stderr_path": self.stderr_path,
            "skip_reason": self.skip_reason,
            "state_marker": self.marker,
            "rivet_version": self.rivet_version,
        }


@dataclass
class _Step:
    """One finished rivet process."""

    exit_status: Optional[int]
    signal: Optional[int]
    line: Optional[dict[str, Any]]
    stdout_path: Path


class _Session:
    """One task's invocation: its preflight result, working files and collected degradations."""

    def __init__(self, request: Request, pf: Preflight, emit: Emit, on_process: Optional[Callable]) -> None:
        self.req = request
        self.pf = pf
        self.emit = emit
        self.on_process = on_process
        self.degraded: set[str] = set()
        self.argvs: list[list[str]] = []
        self.stderr_path: Optional[str] = None
        self.tmp: Optional[Path] = None
        self.name = "__".join(_SAFE.sub("_", str(part)) for part in request.label[1:])
        dag = _SAFE.sub("_", str(request.label[0]))
        if pf.state_dir is not None:
            self.work = pf.state_dir / "airflow" / dag
            logs = Path(request.stderr_dir) if request.stderr_dir else pf.state_dir / "logs" / dag
        else:
            self.tmp = Path(tempfile.mkdtemp(prefix="rivet-airflow-"))
            self.work = self.tmp
            logs = Path(request.stderr_dir) if request.stderr_dir else self.tmp
        self.logs = logs
        self.keep_logs = pf.state_dir is not None or request.stderr_dir is not None
        self.work.mkdir(parents=True, exist_ok=True)
        self.logs.mkdir(parents=True, exist_ok=True)

    def close(self) -> None:
        """Drop the scratch directory of a diskless worker."""
        if self.tmp is not None:
            shutil.rmtree(self.tmp, ignore_errors=True)

    def config_for(self, only_export: Optional[str]) -> str:
        """The config path rivet reads: a copy in the state directory, narrowed to one export when asked."""
        source = Path(self.req.config).resolve()
        target_dir = self.pf.state_dir if self.pf.state_dir is not None else None
        if target_dir is not None and self.pf.state_kind == "sqlite":
            self.degraded.add("state_url_sqlite")
        relative_query = any(
            isinstance(e, dict) and e.get("query_file") and not os.path.isabs(str(e["query_file"]))
            for e in self.pf.config.get("exports") or []
        )
        if only_export is None and (target_dir is None or source.parent == target_dir.resolve()):
            return str(source)
        if only_export is None and not relative_query:
            return _write_atomic(target_dir / source.name, source.read_bytes())
        doc = dict(self.pf.config)
        exports = [dict(e) for e in doc.get("exports") or [] if isinstance(e, dict)]
        for entry in exports:
            if entry.get("query_file") and not os.path.isabs(str(entry["query_file"])):
                entry["query_file"] = str(source.parent / str(entry["query_file"]))
        if only_export is not None:
            exports = [e for e in exports if e.get("name") == only_export]
        doc["exports"] = exports
        suffix = f"--{_SAFE.sub('_', only_export)}" if only_export is not None else ""
        name = f"{source.stem}{suffix}{source.suffix or '.yaml'}"
        return _write_atomic((target_dir or self.work) / name, _yaml.dump(doc).encode())

    def globals_for(self, subcommand: str) -> list[str]:
        """The global flags this binary accepts; a missing one is recorded, never passed."""
        flags = ["--json-errors"]
        if self.pf.caps.has_flag(subcommand, "--no-notify"):
            flags.append("--no-notify")
        else:
            self.degraded.add("no_notify")
        if self.pf.caps.has_flag(subcommand, "--lock-wait"):
            flags += ["--lock-wait", str(self.req.lock_wait)]
        else:
            self.degraded.add("lock_wait")
        return flags

    def step(self, name: str, argv: list[str], stdout_to: Optional[Path] = None) -> _Step:
        """Run one rivet process with stdout and stderr sent to files, and read its error line."""
        self.argvs.append(argv)
        stderr_path = self.logs / f"{self.name}.{name}.stderr.log"
        stdout_path = stdout_to or self.logs / f"{self.name}.{name}.stdout.log"
        shown = str(stderr_path) if self.keep_logs else None
        self.emit({"event": "rivet.start", "step": name, "argv": argv, "stderr_path": shown})
        cwd = self.req.cwd or str(Path(self.req.config).resolve().parent)
        with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err:
            proc = subprocess.Popen(argv, stdout=out, stderr=err, stdin=subprocess.DEVNULL, env=dict(self.req.env), cwd=cwd)
            if self.on_process:
                self.on_process(proc)
            try:
                code = proc.wait()
            except BaseException:
                _stop(proc)
                raise
        if name != "metrics":
            self.stderr_path = shown
        text = stderr_path.read_text(errors="replace")
        if not self.keep_logs:
            stream = sys.__stderr__ or sys.stderr
            stream.write(text)
            stream.flush()
        if self.req.stream_stderr:
            for raw in text.splitlines():
                self.emit({"event": "rivet.stderr", "step": name, "line": raw})
        signal = -code if code < 0 else None
        return _Step(None if signal else code, signal, _error_line(text), stdout_path)

    def classify(self, step: _Step) -> Optional[ErrorObject]:
        """The process-level error object: read when rivet printed one, built from the exit when not."""
        if step.signal is None and step.exit_status == 0:
            return None
        if step.signal is None and step.line is not None:
            error, derived = error_from_line(step.line)
            if derived:
                self.degraded.add("error_object")
            return error
        return error_from_exit(step.exit_status, step.signal)

    def unit_error(self, step: _Step, own: Any, export: Optional[str], table: Optional[str], shared: ErrorObject) -> ErrorObject:
        """A failed unit's object: its own, its `failures[]` entry, or the process-level one (recorded)."""
        if isinstance(own, dict) and "class" in own:
            return ErrorObject.from_contract(own)
        for entry in (step.line or {}).get("failures") or []:
            if entry.get("export") == export and entry.get("table") == table and "class" in entry:
                return ErrorObject.from_contract(entry)
        self.degraded.add("per_unit_error")
        return shared

    def outcome(self, step: _Step, units: list[UnitResult], error: Optional[ErrorObject]) -> Outcome:
        """Assemble the task's result and log one structured line per unit."""
        out = Outcome(
            command=self.req.command,
            units=units,
            error=error,
            exit_status=step.exit_status,
            signal=step.signal,
            degraded=sorted(self.degraded),
            stderr_path=self.stderr_path,
            argv=self.argvs,
            marker=self.pf.marker,
            rivet_version=self.pf.caps.version_text,
        )
        if len(units) == 1:
            out.skip_reason = units[0].skip_reason
        for unit in units:
            record = unit.to_xcom()
            record.update(record.pop("error") or {})
            self.emit({"event": "rivet.unit", "skip_reason": unit.skip_reason, **record})
        self.emit(
            {
                "event": "rivet.result",
                "command": out.command,
                "decision": out.decision,
                "degraded": out.degraded,
                "exit_status": out.exit_status,
                "signal": out.signal,
                "stderr_path": out.stderr_path,
            }
        )
        return out


def _write_atomic(path: Path, data: bytes) -> str:
    """Write a file by temp file and rename, so parallel tasks never read a partial copy."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    os.replace(tmp, path)
    return str(path)


def _stop(proc: subprocess.Popen) -> None:
    """Terminate a rivet process the task is abandoning, then kill it if it lingers."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _error_line(stderr_text: str) -> Optional[dict[str, Any]]:
    """The last stderr line that is a JSON object with `exit_class`: the `--json-errors` line."""
    for raw in reversed(stderr_text.splitlines()[-200:]):
        raw = raw.strip()
        if not raw.startswith("{"):
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        if isinstance(obj, dict) and "exit_class" in obj:
            return obj
    return None


def _read_json(path: Path) -> Any:
    """Parse a JSON file rivet wrote, `None` when it is absent or not JSON."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _unexpired(artifact: Path) -> bool:
    """True when a sealed plan artifact exists and its `expires_at` is still ahead."""
    doc = _read_json(artifact)
    try:
        expires = datetime.fromisoformat(str(doc["expires_at"]).replace("Z", "+00:00"))
    except (TypeError, KeyError, ValueError):
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return expires > datetime.now(timezone.utc)


def _run_units(s: _Session, step: _Step, summary: Any, error: Optional[ErrorObject]) -> list[UnitResult]:
    """Units of a `run` / `apply` process, from its summary file when it wrote one."""
    entries = summary.get("per_export") if isinstance(summary, dict) else None
    if not entries:
        if error is None and s.req.export is None:
            return []
        return [UnitResult(s.req.export, status="failed" if error else "success", error=error)]
    units = []
    for entry in entries:
        name = entry.get("export_name")
        failed = entry.get("status") == "failed"
        unit_error = None
        if failed:
            shared = error or error_from_exit(step.exit_status, step.signal)
            unit_error = s.unit_error(step, entry.get("error"), name, None, shared)
        if entry.get("mode") == "cdc" and entry.get("status") == "success" and "stop_reason" not in entry:
            s.degraded.add("stop_reason")
        units.append(
            UnitResult(
                export=name,
                status=entry.get("status", "failed"),
                run_id=entry.get("run_id"),
                rows=entry.get("rows"),
                files=entry.get("files"),
                stop_reason=entry.get("stop_reason"),
                error=unit_error,
            )
        )
    if len([u for u in units if u.status == "failed"]) <= 1:
        s.degraded.discard("per_unit_error")
    return units


def _run(s: _Session) -> Outcome:
    """`rivet run`, for one export or the whole config."""
    cfg = s.config_for(None)
    summary = s.work / f"{s.name}.summary.json"
    argv = [s.pf.binary, "run", "--config", cfg]
    if s.req.export:
        argv += ["--export", s.req.export]
    argv += ["--summary-output", str(summary), *s.req.extra_args, *s.globals_for("run")]
    step = s.step("run", argv)
    error = s.classify(step)
    doc = _read_json(summary)
    summary.unlink(missing_ok=True)
    return s.outcome(step, _run_units(s, step, doc, error), error)


def _apply(s: _Session) -> Outcome:
    """`rivet plan -e X -o artifact` when no unexpired artifact exists, then `rivet apply artifact`."""
    export = s.req.export or ""
    cfg = s.config_for(None)
    run_part = _SAFE.sub("_", str(s.req.label[1]))
    artifact = s.work / "plans" / run_part / f"{_SAFE.sub('_', export)}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if not _unexpired(artifact):
        plan_argv = [s.pf.binary, "plan", "--config", cfg, "--export", export, "--format", "json"]
        plan_argv += ["--output", str(artifact), "--json-errors"]
        planned = s.step("plan", plan_argv)
        error = s.classify(planned)
        if error is not None:
            return s.outcome(planned, [UnitResult(export, status="failed", error=error)], error)
    has_summary = s.pf.caps.has_flag("apply", "--summary-output")
    summary = s.work / f"{s.name}.summary.json"
    argv = [s.pf.binary, "apply", str(artifact)]
    if has_summary:
        argv += ["--summary-output", str(summary)]
    else:
        s.degraded.add("apply_summary")
    argv += [*s.req.extra_args, *s.globals_for("apply")]
    step = s.step("apply", argv)
    error = s.classify(step)
    doc = _read_json(summary) if has_summary else None
    summary.unlink(missing_ok=True)
    units = _run_units(s, step, doc, error)
    if error is None and not has_summary:
        units = [_unit_from_metrics(s, cfg, export)]
    return s.outcome(step, units, error)


def _unit_from_metrics(s: _Session, cfg: str, export: str) -> UnitResult:
    """Counts of the export's latest run from `rivet metrics --json`; best effort."""
    argv = [s.pf.binary, "metrics", "--config", cfg, "--export", export, "--last", "1", "--json"]
    out = s.work / f"{s.name}.metrics.json"
    step = s.step("metrics", argv, stdout_to=out)
    rows = _read_json(out)
    out.unlink(missing_ok=True)
    unit = UnitResult(export, status="success")
    if step.exit_status == 0 and isinstance(rows, list) and rows and rows[0].get("export_name") == export:
        row = rows[0]
        unit.run_id, unit.rows, unit.files = row.get("run_id"), row.get("total_rows"), row.get("files_produced")
    return unit


def _plan(s: _Session) -> Outcome:
    """`rivet plan --format json` for the whole config, swapped into `plan_file` only when valid."""
    target = Path(str(s.req.plan_file))
    scratch = target.with_name(target.name + ".tmp")
    argv = [s.pf.binary, "plan", "--config", s.config_for(None), "--format", "json", *s.req.extra_args, "--json-errors"]
    step = s.step("plan", argv, stdout_to=scratch)
    error = s.classify(step)
    if error is None:
        if isinstance(_read_json(scratch), list):
            os.replace(scratch, target)
        else:
            error = ErrorObject(None, None, "internal", 6, False, None, "rivet plan printed no JSON plan list")
    scratch.unlink(missing_ok=True)
    return s.outcome(step, [UnitResult(None, status="failed" if error else "success", error=error)], error)


def _load(s: _Session) -> Outcome:
    """`rivet load` / `rivet compact` for one export and, where the binary can, one table."""
    cmd, export, table = s.req.command, s.req.export, s.req.table
    by_export = s.pf.caps.has_flag(cmd, "--export")
    by_table = s.pf.caps.has_flag(cmd, "--table")
    has_result = s.pf.caps.has_flag(cmd, "--summary-output")
    if export and not by_export:
        s.degraded.add("load_filter")
    argv = [s.pf.binary, cmd, "--config", s.config_for(export if export and not by_export else None)]
    if export and by_export:
        argv += ["--export", export]
    if table and by_table:
        argv += ["--table", table]
    result = s.work / f"{s.name}.result.json"
    if has_result:
        argv += ["--summary-output", str(result)]
    else:
        s.degraded.add("load_result")
    argv += [*s.req.extra_args, *s.globals_for(cmd)]
    lock = None
    if table and not by_table:
        s.degraded.add("load_table_filter")
        if s.pf.state_dir is not None:
            locks = s.pf.state_dir / "airflow" / "locks"
            locks.mkdir(parents=True, exist_ok=True)
            lock = open(locks / f"{_SAFE.sub('_', export or 'config')}.lock", "w")
            fcntl.flock(lock, fcntl.LOCK_EX)
    try:
        step = s.step(cmd, argv)
    finally:
        if lock is not None:
            lock.close()
    error = s.classify(step)
    doc = _read_json(result) if has_result else None
    result.unlink(missing_ok=True)
    rows = doc.get("per_table") if isinstance(doc, dict) else None
    if not rows:
        status = "failed" if error else DONE_WORD[cmd]
        return s.outcome(step, [UnitResult(export, table, status=status, error=error)], error)
    units = []
    for row in rows:
        if table and row.get("table") != table:
            continue
        unit_error = None
        if row.get("status") == "failed":
            shared = error or error_from_exit(step.exit_status, step.signal)
            unit_error = s.unit_error(step, row.get("error"), row.get("export"), row.get("table"), shared)
        units.append(
            UnitResult(
                export=row.get("export"),
                table=row.get("table"),
                status=row.get("status", "failed"),
                run_id=doc.get("run_id"),
                rows=row.get("rows"),
                error=unit_error,
                skip_reason=row.get("skip_reason"),
            )
        )
    if len([u for u in units if u.status == "failed"]) <= 1:
        s.degraded.discard("per_unit_error")
    return s.outcome(step, units, error)


_DISPATCH = {"run": _run, "cdc_run": _run, "apply": _apply, "plan": _plan, "load": _load, "compact": _load}


def invoke(request: Request, emit: Emit, on_process: Optional[Callable] = None) -> Outcome:
    """Preflight, run and classify one rivet task."""
    if request.command not in _DISPATCH:
        raise ValueError(f"unknown rivet command {request.command!r}")
    pf = run_preflight(
        rivet_bin=request.rivet_bin,
        config_path=request.config,
        env=request.env,
        state_dir=request.state_dir,
        deployment=request.deployment,
        export=request.export,
        cdc=request.command == "cdc_run",
        upstream_markers=tuple(request.upstream_markers),
    )
    for key, text in pf.warnings:
        emit({"event": "rivet.warning", "warning": key, "text": text})
    session = _Session(request, pf, emit, on_process)
    try:
        return _DISPATCH[request.command](session)
    finally:
        session.close()


def crashed_retry_allowed(
    ledger: Optional[Path], try_number: int, max_tries: int, retries: int, budget: int
) -> bool:
    """Whether a crash on this try is retried: at most `budget` earlier crashes since the last clear."""
    series_start = max(0, max_tries - retries)
    if ledger is None:
        return try_number - series_start <= budget
    ledger.parent.mkdir(parents=True, exist_ok=True)
    seen = _read_json(ledger)
    tries = [t for t in seen if isinstance(t, int)] if isinstance(seen, list) else []
    earlier = [t for t in tries if series_start < t < try_number]
    _write_atomic(ledger, json.dumps(sorted(set(tries) | {try_number})).encode())
    return len(earlier) < budget
