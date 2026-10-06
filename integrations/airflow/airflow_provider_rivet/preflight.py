"""The checks every rivet task passes before its subprocess starts (ADR-0039 D5, D9)."""

from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

from . import _yaml
from .capabilities import MIN_RIVET_VERSION, Capabilities, probe

MARKER_NAME = ".rivet_airflow_marker"
STATE_DB_NAME = ".rivet_state.db"
SETUP_GUIDE = "integrations/airflow/README.md#local-worker-setup-guide"


class PreflightRefusal(Exception):
    """A task the package refuses to start; never retried."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code


@dataclass
class Preflight:
    """What the checks established about the binary, the deployment and the state."""

    binary: str
    caps: Capabilities
    deployment: str
    state_kind: str
    state_dir: Optional[Path]
    config: dict[str, Any]
    marker: Optional[str] = None
    warnings: list[tuple[str, str]] = field(default_factory=list)

    def export_modes(self) -> dict[str, str]:
        """Mode by export name, as the config declares it."""
        exports = self.config.get("exports") or []
        return {e["name"]: str(e.get("mode", "full")) for e in exports if isinstance(e, dict) and "name" in e}


def detect_deployment(env: Mapping[str, str], declared: str = "auto") -> str:
    """`pod` for an ephemeral Kubernetes worker, `local` for a host that keeps its disk."""
    if declared in ("local", "pod"):
        return declared
    return "pod" if env.get("KUBERNETES_SERVICE_HOST") else "local"


def resolve_binary(rivet_bin: str, env: Mapping[str, str]) -> str:
    """The absolute path of the rivet binary, or a refusal naming where it was looked for."""
    found = shutil.which(rivet_bin, path=env.get("PATH"))
    if not found:
        raise PreflightRefusal("RIVET_AIRFLOW_BINARY_MISSING", f"no rivet binary `{rivet_bin}` on the worker's PATH")
    return found


def read_config(config_path: str) -> dict[str, Any]:
    """Parse the config as YAML; a refusal when it is missing or not a mapping."""
    try:
        doc = _yaml.load(Path(config_path).read_text())
    except (OSError, _yaml.yaml.YAMLError) as exc:
        raise PreflightRefusal("RIVET_AIRFLOW_CONFIG_UNREADABLE", f"cannot read config {config_path}: {type(exc).__name__}") from exc
    if not isinstance(doc, dict):
        raise PreflightRefusal("RIVET_AIRFLOW_CONFIG_UNREADABLE", f"config {config_path} is not a YAML mapping")
    return doc


def ensure_marker(state_dir: Path) -> str:
    """Read the directory's marker, creating it on the first task that sees the directory."""
    marker = state_dir / MARKER_NAME
    try:
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return marker.read_text().strip()
    with os.fdopen(fd, "w") as handle:
        value = uuid.uuid4().hex
        handle.write(value)
    return value


def check_state_dir(state_dir: Optional[str]) -> Path:
    """The declared directory must be absolute, exist and be writable; it is never created here."""
    if not state_dir:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_STATE_DIR_REQUIRED",
            f"this task keeps state or a CDC checkpoint on the worker and needs `state_dir`; see {SETUP_GUIDE}",
        )
    path = Path(state_dir)
    if not path.is_absolute() or not path.is_dir():
        raise PreflightRefusal(
            "RIVET_AIRFLOW_STATE_DIR_MISSING",
            f"state directory {state_dir} is not an existing absolute directory on this worker; "
            f"it is never created by a task, because an empty one reads as a first run; see {SETUP_GUIDE}",
        )
    if not os.access(path, os.W_OK | os.X_OK):
        raise PreflightRefusal("RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"state directory {state_dir} is not writable")
    return path


def check_nothing_left_beside_config(config_path: str, config: dict[str, Any], state_dir: Path, sqlite: bool, export: Optional[str]) -> None:
    """Refuse when state or a CDC checkpoint sits beside the original config and the copy in `state_dir` would not see it."""
    origin = Path(config_path).resolve().parent
    if origin == state_dir.resolve():
        return
    left = origin / STATE_DB_NAME
    if sqlite and left.exists() and not (state_dir / STATE_DB_NAME).exists():
        raise PreflightRefusal(
            "RIVET_AIRFLOW_STATE_BESIDE_CONFIG",
            f"{left} exists and {state_dir} has no {STATE_DB_NAME}: rivet would start from an empty state there and "
            f"treat every export as a first run. Stop every run of this config, then move the file (with its -wal and "
            f"-shm files) into {state_dir}; or delete it if that state is not this pipeline's; see {SETUP_GUIDE}",
        )
    for entry in config.get("exports") or []:
        cdc = entry.get("cdc") if isinstance(entry, dict) else None
        ref = cdc.get("checkpoint") if isinstance(cdc, dict) else None
        if not ref or os.path.isabs(str(ref)) or export not in (None, entry.get("name")):
            continue
        if (origin / str(ref)).exists() and not (state_dir / str(ref)).exists():
            raise PreflightRefusal(
                "RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG",
                f"the checkpoint of `{entry.get('name')}` is at {origin / str(ref)}, and under Airflow the relative "
                f"`cdc.checkpoint` resolves in the state directory: a stream that starts without its checkpoint "
                f"re-anchors and skips the changes in between. Stop the stream, then move the file to "
                f"{state_dir / str(ref)}; see {SETUP_GUIDE}",
            )


def run_preflight(
    *,
    rivet_bin: str,
    config_path: str,
    env: Mapping[str, str],
    state_dir: Optional[str],
    deployment: str = "auto",
    export: Optional[str] = None,
    cdc: bool = False,
    upstream_markers: tuple[str, ...] = (),
) -> Preflight:
    """Run every check; raise `PreflightRefusal` or return what the invocation needs."""
    binary = resolve_binary(rivet_bin, env)
    try:
        caps = probe(binary)
    except (OSError, RuntimeError) as exc:
        raise PreflightRefusal("RIVET_AIRFLOW_BINARY_MISSING", str(exc)) from exc
    if not caps.new_enough():
        floor = ".".join(str(n) for n in MIN_RIVET_VERSION)
        raise PreflightRefusal(
            "RIVET_AIRFLOW_BINARY_TOO_OLD",
            f"rivet {caps.version_text} at {binary} is older than {floor}, the minimum this package runs",
        )
    config = read_config(config_path)
    where = detect_deployment(env, deployment)
    state_kind = "postgres" if env.get("RIVET_STATE_URL", "").startswith("postgres") else "sqlite"
    result = Preflight(binary, caps, where, state_kind, None, config)
    modes = result.export_modes()
    if export is not None and modes and export not in modes:
        raise PreflightRefusal("RIVET_AIRFLOW_EXPORT_UNKNOWN", f"config {config_path} has no export `{export}`")
    touched = [modes.get(export, "")] if export is not None else list(modes.values())
    is_cdc = cdc or "cdc" in touched

    if where == "pod":
        if state_kind != "postgres":
            raise PreflightRefusal(
                "RIVET_AIRFLOW_STATE_SQLITE_ON_POD",
                "this worker is an ephemeral pod and RIVET_STATE_URL is not a postgres URL; SQLite state would be "
                "lost with the pod and every later task would start from an empty state",
            )
        if is_cdc:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_CDC_ON_POD",
                "CDC is refused on an ephemeral pod: the stream's checkpoint is a file and would live on the pod's "
                "disk; this holds until rivet stores the checkpoint in the state database",
            )
    elif state_kind == "sqlite" or is_cdc or state_dir:
        result.state_dir = check_state_dir(state_dir)
        result.marker = ensure_marker(result.state_dir)
        others = sorted({m for m in upstream_markers if m and m != result.marker})
        if others:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_STATE_DIR_DIFFERS",
                f"an upstream task of this DAG run used another state directory than {state_dir} on this worker "
                f"(marker {others[0][:8]} against {result.marker[:8]}); every task must see the same path; see {SETUP_GUIDE}",
            )
        check_nothing_left_beside_config(config_path, config, result.state_dir, state_kind == "sqlite", export)
        if state_kind == "sqlite":
            result.warnings.append(
                ("sqlite_single_host", f"SQLite state in {state_dir}: this setup is single-host; see {SETUP_GUIDE}")
            )

    if "notifications" in config and not caps.has_flag("run", "--no-notify"):
        result.warnings.append(
            (
                "notifications_block",
                "the config has a `notifications:` block and this rivet has no `--no-notify`: rivet will send its own "
                "messages beside Airflow's alerts; remove the block from configs run under Airflow",
            )
        )
    return result
