"""The one place a rivet process is built, run, read and classified; it imports nothing from Airflow."""

from __future__ import annotations

import fcntl
import fnmatch
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import _yaml
from .preflight import Preflight, PreflightRefusal, run_preflight
from .result import STATUSES, ErrorObject, UnitResult, error_from_exit, error_from_line, worst

Emit = Callable[[dict[str, Any]], None]
COMMANDS = ("run", "cdc_run", "apply", "plan", "load", "compact")
DONE_WORD = {"load": "loaded", "compact": "compacted"}
ENV_ALLOWLIST = (
    "PATH", "HOME", "USER", "LOGNAME", "TZ", "LANG", "LC_*", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "no_proxy", "all_proxy",
    "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
)  # fmt: skip
CLOUD_ENV = {
    "gcp": ("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT"),
    "aws": ("AWS_*",),
    "azure": ("AZURE_*",),
}
STDERR_TAIL = 1 << 20
QUERY_OWNERS = ".rivet_airflow_query_files.json"
PLAN_ERRORS = (ValueError, KeyError, IndexError, TypeError, AttributeError)
STATE_ENV = ("RIVET_STATE_URL",)
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")
_PLAIN = re.compile(r"[a-z0-9_][a-z0-9_.-]{0,79}")
_HASHED = re.compile(r".*-[0-9a-f]{12}")
_LOG_FILE = re.compile(r"^(?P<attempt>.+)\.[a-z]+\.(?:stderr|stdout)\.log$")


def worker_environment(
    passthrough: Sequence[str] = (), clouds: Sequence[str] = (), environ: Optional[Mapping[str, str]] = None
) -> dict[str, str]:
    """The worker's variables a rivet process may inherit: the allow-list, plus the named extras."""
    patterns = [*ENV_ALLOWLIST, *passthrough, *(name for cloud in clouds for name in CLOUD_ENV[cloud])]
    source = os.environ if environ is None else environ
    return {k: v for k, v in source.items() if any(fnmatch.fnmatchcase(k, pattern) for pattern in patterns)}


def path_name(*parts: Any) -> str:
    """One path component for user-supplied names, never shared by two different inputs.

    A lone lower-case name of safe characters is kept as it is. Anything else (other characters, upper case,
    several parts, a leading dot, over 80 characters, or a name shaped like this function's own output) becomes
    a readable form plus 12 hex digits of the SHA-256 of the exact input.
    """
    texts = [str(part) for part in parts]
    if len(texts) == 1 and _PLAIN.fullmatch(texts[0]) and not _HASHED.fullmatch(texts[0]):
        return texts[0]
    readable = "__".join(_SAFE.sub("_", text) for text in texts)[:80]
    return f"{readable}-{hashlib.sha256(json.dumps(texts).encode()).hexdigest()[:12]}"


def undeclared_state_variables(env: Mapping[str, str], environ: Optional[Mapping[str, str]] = None) -> list[str]:
    """Names of the worker's state-selecting variables that the task's environment would not carry to rivet."""
    source = os.environ if environ is None else environ
    return [name for name in STATE_ENV if source.get(name) and name not in env]


def plan_layout(plan: Any) -> list[dict[str, Any]]:
    """Waves of a `rivet plan --format json` document, lowest first; raises one of PLAN_ERRORS when it is not one."""
    campaign = plan[0]["prioritization"]["campaign"]
    cost = {e["export_name"]: e.get("cost_class") for e in campaign.get("ordered_exports", [])}
    waves = sorted(campaign["waves"], key=lambda w: w["wave"])
    layout = [
        {"wave": w["wave"], "exports": list(w["exports"]), "heavy": [n for n in w["exports"] if cost.get(n) != "low"]}
        for w in waves
        if w["exports"]
    ]
    if not layout:
        raise ValueError("the plan has no wave with an export")
    return layout


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
    log_keep: Optional[int] = 10
    label: Sequence[Any] = ("dag", "run", "task", 1)
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
        dag, run, task = (path_name(part) for part in request.label[:3])
        attempt = f"try{int(request.label[3])}"
        self.run = run
        self.name = path_name(*request.label[1:3], attempt)
        self.log_name = f"{run}__{attempt}"
        if pf.state_dir is not None:
            self.work = _private_dir(pf.state_dir, "airflow", dag)
        else:
            self.tmp = Path(tempfile.mkdtemp(prefix="rivet-airflow-"))
            self.work = self.tmp
        if request.stderr_dir:
            Path(request.stderr_dir).mkdir(parents=True, exist_ok=True)
            self.logs = _private_dir(Path(request.stderr_dir), dag, task)
        elif pf.state_dir is not None:
            self.logs = _private_dir(pf.state_dir, "logs", dag, task)
        else:
            self.logs = _private_dir(_temp_base(), "logs", dag, task)
            self.degraded.add("stderr_temp")

    def close(self) -> None:
        """Drop the scratch directory of a diskless worker and the log files past the retention."""
        if self.tmp is not None:
            shutil.rmtree(self.tmp, ignore_errors=True)
        if self.req.log_keep is not None:
            _prune_logs(self.logs, max(1, self.req.log_keep))

    def config_for(self, only_export: Optional[str]) -> str:
        """The config path rivet reads: a copy in the state directory, narrowed to one export when asked."""
        source = Path(self.req.config).resolve()
        target_dir = self.pf.state_dir
        if target_dir is not None and self.pf.state_kind == "sqlite":
            self.degraded.add("state_url_sqlite")
        if only_export is None and (target_dir is None or source.parent == target_dir.resolve()):
            return str(source)
        folder = target_dir if target_dir is not None else self.work
        if folder.resolve() != source.parent:
            _copy_query_files(self.pf.config, source, folder, self.req.export)
        if only_export is None:
            return _write_atomic(folder / copy_name(source), source.read_bytes())
        doc = dict(self.pf.config)
        doc["exports"] = [e for e in doc.get("exports") or [] if isinstance(e, dict) and e.get("name") == only_export]
        return _write_atomic(folder / copy_name(source, only_export), _yaml.dump(doc).encode())

    def malformed(self, value: Any, kind: type) -> Any:
        """A value of a result file when it has the type the contract gives it, else `None`, recorded."""
        if value is None or type(value) is kind:
            return value
        self.degraded.add("result_malformed")
        return None

    def status(self, value: Any) -> str:
        """A unit's status word; anything outside the contract's sets counts as failed, recorded."""
        if isinstance(value, str) and value in STATUSES:
            return value
        self.degraded.add("result_malformed")
        return "failed"

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
        """Run one rivet process with stdout and stderr sent to private files, and read its error line."""
        self.argvs.append(argv)
        stderr_path = self.logs / f"{self.log_name}.{name}.stderr.log"
        stdout_path = stdout_to or self.logs / f"{self.log_name}.{name}.stdout.log"
        self.emit({"event": "rivet.start", "step": name, "argv": argv, "stderr_path": str(stderr_path)})
        cwd = self.req.cwd or str(Path(self.req.config).resolve().parent)
        with _open_private(stdout_path) as out, _open_private(stderr_path) as err:
            proc = subprocess.Popen(argv, stdout=out, stderr=err, stdin=subprocess.DEVNULL, env=dict(self.req.env), cwd=cwd)
            if self.on_process:
                self.on_process(proc)
            try:
                code = proc.wait()
            except BaseException:
                _stop(proc)
                raise
        if name != "metrics":
            self.stderr_path = str(stderr_path)
        text = _tail(stderr_path)
        if self.req.stream_stderr:
            for raw in text.splitlines():
                self.emit({"event": "rivet.stderr", "step": name, "line": raw})
        signal = -code if code < 0 else None
        return _Step(None if signal else code, signal, _error_line(text), stdout_path)

    def classify(self, step: _Step) -> Optional[ErrorObject]:
        """The process-level error object: read when rivet printed a well-formed one, built from the exit when not."""
        if step.signal is None and step.exit_status == 0:
            return None
        if step.signal is None and step.line is not None:
            error, derived = error_from_line(step.line, step.exit_status)
            if error is not None:
                if derived:
                    self.degraded.add("error_object")
                return error
            self.degraded.add("result_malformed")
        return error_from_exit(step.exit_status, step.signal)

    def unit_error(self, step: _Step, own: Any, export: Optional[str], table: Optional[str], shared: ErrorObject) -> ErrorObject:
        """A failed unit's object: its own, its `failures[]` entry, or the process-level one (recorded)."""
        failures = (step.line or {}).get("failures")
        candidates = [own] if own is not None else []
        for entry in failures if isinstance(failures, list) else []:
            if not isinstance(entry, dict):
                self.degraded.add("result_malformed")
            elif entry.get("export") == export and entry.get("table") == table:
                candidates.append(entry)
        for candidate in candidates:
            parsed = ErrorObject.from_contract(candidate)
            if parsed is not None:
                return parsed
            self.degraded.add("result_malformed")
        self.degraded.add("per_unit_error")
        return shared

    def outcome(self, step: _Step, units: list[UnitResult], error: Optional[ErrorObject]) -> Outcome:
        """Assemble the task's result and log one structured line per unit; a failed unit fails the task."""
        failed = [u.error for u in units if u.status == "failed" and u.error is not None]
        if error is None and failed:
            error = worst(failed)
            self.degraded.add("exit_zero_failed_unit")
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
    """Write a 0600 file by temp file and rename, so parallel tasks never read a partial copy."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    os.replace(tmp, path)
    return str(path)


def _open_private(path: Path) -> Any:
    """Open a file for writing with mode 0600, whatever mode an earlier file of that name had."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "wb")


def _private_dir(base: Path, *parts: str) -> Path:
    """Create the directories below `base` with mode 0700 and tighten the ones that exist."""
    path = base
    for part in parts:
        path = path / part
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise PreflightRefusal("RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"cannot create {path}: {type(exc).__name__}") from exc
        try:
            if stat.S_IMODE(os.stat(path).st_mode) != 0o700:
                os.chmod(path, 0o700)
        except OSError as exc:
            raise PreflightRefusal("RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"cannot make {path} private: {type(exc).__name__}") from exc
    return path


def _temp_base() -> Path:
    """This user's private directory under the temp directory; a fresh random one when that name is not safely ours."""
    path = Path(tempfile.gettempdir()) / f"rivet-airflow-{os.getuid()}"
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        info = os.lstat(path)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            return Path(tempfile.mkdtemp(prefix="rivet-airflow-"))
    return path


def _prune_logs(folder: Path, keep: int) -> None:
    """Keep the stdout and stderr files of a task's newest `keep` tries; remove the older tries' files."""
    attempts: dict[str, list[Path]] = {}
    try:
        for entry in folder.iterdir():
            match = _LOG_FILE.match(entry.name)
            if match:
                attempts.setdefault(match["attempt"], []).append(entry)
        newest_last = sorted(attempts, key=lambda a: max(f.stat().st_mtime_ns for f in attempts[a]))
        for attempt in newest_last[:-keep]:
            for entry in attempts[attempt]:
                entry.unlink(missing_ok=True)
    except OSError:
        return


def _tail(path: Path) -> str:
    """The last STDERR_TAIL bytes of a file, as text."""
    with open(path, "rb") as handle:
        size = handle.seek(0, os.SEEK_END)
        handle.seek(max(0, size - STDERR_TAIL))
        return handle.read().decode(errors="replace")


def copy_name(source: Path, only_export: Optional[str] = None) -> str:
    """The name of a config's copy: its own stem plus a hash of its absolute path, so two sources never share one."""
    digest = hashlib.sha256(str(source).encode()).hexdigest()[:8]
    suffix = f"--{path_name(only_export)}" if only_export is not None else ""
    return f"{source.stem}.{digest}{suffix}{source.suffix or '.yaml'}"


def _copy_query_files(config: Mapping[str, Any], source: Path, folder: Path, touched: Optional[str]) -> None:
    """Copy every relative `query_file` to the same relative path beside the config's copy; refuse what rivet would."""
    owners = _read_json(folder / QUERY_OWNERS)
    owners = owners if isinstance(owners, dict) else {}
    claimed = dict(owners)
    for entry in config.get("exports") or []:
        ref = entry.get("query_file") if isinstance(entry, dict) else None
        if not ref:
            continue
        name, ref, origin = entry.get("name"), str(ref), source.parent / str(ref)

        def refuse(why: str) -> None:
            raise PreflightRefusal("RIVET_AIRFLOW_QUERY_FILE", f"export `{name}` of {source}: query_file `{ref}` {why}")

        if os.path.isabs(ref) or ".." in Path(ref).parts:
            refuse("must be a relative path with no `..`: rivet reads it only inside the config's directory")
        if not origin.is_file():
            if touched in (None, name):
                refuse("is not a file beside the config")
            continue
        if source.parent not in origin.resolve().parents:
            refuse("resolves outside the config's directory, which rivet refuses")
        if claimed.get(ref, str(source.parent)) != str(source.parent):
            refuse(
                f"is already used in {folder} by a config of {claimed[ref]}; two config directories that share a "
                "state directory cannot use one relative path: give them separate state directories"
            )
        claimed[ref] = str(source.parent)
        _private_dir(folder, *Path(ref).parts[:-1])
        _write_atomic(folder / ref, origin.read_bytes())
    if claimed != owners:
        _write_atomic(folder / QUERY_OWNERS, json.dumps(claimed, sort_keys=True).encode())


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
    """The last line of the stderr tail that is a JSON object with `exit_class`: the `--json-errors` line."""
    for raw in reversed(stderr_text.splitlines()):
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


def _refuse_foreign_artifact(artifact: Path, export: str) -> None:
    """Refuse a sealed plan artifact that records another export than the one being applied; the file is left alone."""
    doc = _read_json(artifact)
    if isinstance(doc, dict) and doc.get("export_name") != export:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN",
            f"the sealed plan artifact {artifact} is for export `{doc.get('export_name')}`, not `{export}`: applying "
            "it would extract the other export under this task's name. It is neither applied nor replaced; delete "
            "the file once you know what wrote it",
        )


def _entries(s: _Session, doc: Any, key: str) -> list[dict[str, Any]]:
    """The unit entries of a result file; anything that is not a list of objects is dropped and recorded."""
    entries = doc.get(key) if isinstance(doc, dict) else None
    if entries is None:
        return []
    if not isinstance(entries, list) or not all(isinstance(e, dict) for e in entries):
        s.degraded.add("result_malformed")
    return [e for e in entries if isinstance(e, dict)] if isinstance(entries, list) else []


def _run_units(s: _Session, step: _Step, summary: Any, error: Optional[ErrorObject]) -> list[UnitResult]:
    """Units of a `run` / `apply` process, from its summary file when it wrote one."""
    entries = _entries(s, summary, "per_export")
    if not entries:
        if error is None and s.req.export is None:
            return []
        return [UnitResult(s.req.export, status="failed" if error else "success", error=error)]
    units = []
    for entry in entries:
        name, status = s.malformed(entry.get("export_name"), str), s.status(entry.get("status"))
        unit_error = None
        if status == "failed":
            shared = error or error_from_exit(step.exit_status, step.signal)
            unit_error = s.unit_error(step, entry.get("error"), name, None, shared)
        if entry.get("mode") == "cdc" and status == "success" and "stop_reason" not in entry:
            s.degraded.add("stop_reason")
        units.append(
            UnitResult(
                export=name,
                status=status,
                run_id=s.malformed(entry.get("run_id"), str),
                rows=s.malformed(entry.get("rows"), int),
                files=s.malformed(entry.get("files"), int),
                stop_reason=s.malformed(entry.get("stop_reason"), str),
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
    artifact = _private_dir(s.work, "plans", s.run) / f"{path_name(export)}.json"
    _refuse_foreign_artifact(artifact, export)
    if not _unexpired(artifact):
        plan_argv = [s.pf.binary, "plan", "--config", cfg, "--export", export, "--format", "json"]
        plan_argv += ["--output", str(artifact), "--json-errors"]
        planned = s.step("plan", plan_argv)
        error = s.classify(planned)
        if error is not None:
            return s.outcome(planned, [UnitResult(export, status="failed", error=error)], error)
        _refuse_foreign_artifact(artifact, export)
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
    row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
    if step.exit_status == 0 and row.get("export_name") == export:
        unit.run_id = s.malformed(row.get("run_id"), str)
        unit.rows, unit.files = s.malformed(row.get("total_rows"), int), s.malformed(row.get("files_produced"), int)
    return unit


def _plan(s: _Session) -> Outcome:
    """`rivet plan --format json` for the whole config, swapped into `plan_file` only when a builder can read it."""
    target = Path(str(s.req.plan_file))
    scratch = target.with_name(target.name + ".tmp")
    argv = [s.pf.binary, "plan", "--config", s.config_for(None), "--format", "json", *s.req.extra_args, "--json-errors"]
    step = s.step("plan", argv, stdout_to=scratch)
    error = s.classify(step)
    if error is None:
        try:
            plan_layout(_read_json(scratch))
            os.chmod(scratch, stat.S_IMODE(target.stat().st_mode) if target.exists() else 0o644)
            os.replace(scratch, target)
        except PLAN_ERRORS:
            error = ErrorObject(None, None, "internal", 6, False, None, "rivet plan printed no plan layout")
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
            locks = _private_dir(s.pf.state_dir, "airflow", "locks")
            lock = open(locks / f"{path_name(export or 'config')}.lock", "w")
            fcntl.flock(lock, fcntl.LOCK_EX)
    try:
        step = s.step(cmd, argv)
    finally:
        if lock is not None:
            lock.close()
    error = s.classify(step)
    doc = _read_json(result) if has_result else None
    result.unlink(missing_ok=True)
    rows = _entries(s, doc, "per_table")
    if not rows:
        status = "failed" if error else DONE_WORD[cmd]
        return s.outcome(step, [UnitResult(export, table, status=status, error=error)], error)
    units = []
    for row in rows:
        if table and row.get("table") != table:
            continue
        name, of, status = s.malformed(row.get("export"), str), s.malformed(row.get("table"), str), s.status(row.get("status"))
        unit_error = None
        if status == "failed":
            shared = error or error_from_exit(step.exit_status, step.signal)
            unit_error = s.unit_error(step, row.get("error"), name, of, shared)
        units.append(
            UnitResult(
                export=name,
                table=of,
                status=status,
                run_id=s.malformed(doc.get("run_id"), str),
                rows=s.malformed(row.get("rows"), int),
                error=unit_error,
                skip_reason=s.malformed(row.get("skip_reason"), str),
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
    ledger: Optional[Path], try_number: int, max_tries: int, budget: int
) -> tuple[bool, Optional[str]]:
    """Whether a crash on this try is retried, and what is wrong with the ledger when it grants nothing; never raises."""
    if ledger is None:
        return try_number <= budget, None
    try:
        _private_dir(ledger.parent.parent, ledger.parent.name)
        seen = json.loads(ledger.read_text()) if ledger.exists() else []
    except (OSError, PreflightRefusal) as exc:
        return False, f"crashed ledger {ledger} cannot be written ({type(exc).__name__}): no crash retry is granted"
    except ValueError:
        seen = None
    pairs = isinstance(seen, list) and all(
        isinstance(e, list) and len(e) == 2 and all(type(n) is int for n in e) for e in seen
    )
    if not pairs:
        return False, f"crashed ledger {ledger} is unreadable: no crash retry is granted until it is deleted"
    earlier = [t for t, series in seen if series == max_tries and t < try_number]
    try:
        _write_atomic(ledger, json.dumps(sorted({*map(tuple, seen), (try_number, max_tries)})).encode())
    except OSError as exc:
        return False, f"crashed ledger {ledger} cannot be written ({type(exc).__name__}): no crash retry is granted"
    return len(earlier) < budget, None
