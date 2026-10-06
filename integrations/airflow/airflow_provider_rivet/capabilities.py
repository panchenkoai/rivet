"""What the installed rivet binary supports, and every fallback the package takes when it does not."""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass

MIN_RIVET_VERSION = (0, 31, 0)
_VERSION_RE = re.compile(r"rivet\s+(\d+)\.(\d+)\.(\d+)")


@dataclass(frozen=True)
class Degradation:
    """One thing ADR-0039 promises that a binary may lack, and what the package does instead."""

    key: str
    missing: str
    fallback: str
    detect: str


DEGRADATIONS: dict[str, Degradation] = {
    d.key: d
    for d in (
        Degradation(
            "no_notify",
            "global flag `--no-notify`",
            "flag not passed; a `notifications:` block in the config is warned about",
            "flag",
        ),
        Degradation(
            "lock_wait",
            "global flag `--lock-wait` and the per-export run lease",
            "flag not passed; overlap is limited by the builders' max_active_runs=1 and max_active_tis_per_dag=1",
            "flag",
        ),
        Degradation(
            "state_url_sqlite",
            "`RIVET_STATE_URL=sqlite:<dir>`",
            "the config is materialised into the state directory so the state lands beside it",
            "none",
        ),
        Degradation(
            "error_object",
            "`class` / `retryable` / `kind` / `action` on the `--json-errors` line",
            "class and retryable are derived from the printed line's integer `exit_class`",
            "output",
        ),
        Degradation(
            "per_unit_error",
            "`failures[]` and the per-export `error` object",
            "every failed unit of the process carries the process-level object",
            "output",
        ),
        Degradation(
            "stop_reason",
            "CDC `stop_reason` and `tables[]` on the run entry",
            "`stop_reason` is null; a `max_events` stop cannot be told from `caught_up`",
            "output",
        ),
        Degradation(
            "apply_summary",
            "`--summary-output` on `rivet apply`",
            "counts are read from `rivet metrics --json` after a successful apply",
            "flag",
        ),
        Degradation(
            "load_filter",
            "`--export` on `rivet load` / `rivet compact`",
            "a single-export copy of the config is materialised and loaded instead",
            "flag",
        ),
        Degradation(
            "load_table_filter",
            "`--table` on `rivet load` / `rivet compact`",
            "the task handles every table of its export, serialised by a file lock on a local worker",
            "flag",
        ),
        Degradation(
            "load_result",
            "`--summary-output` on `rivet load` / `rivet compact`",
            "status comes from the exit code only: `skipped` and row counts are unknown",
            "flag",
        ),
    )
}

UNDETECTABLE = (
    "a child killed by a signal is not reported as `crashed` (the parent exits 1 or 2 by text)",
    "no `RIVET_STATE_FOREIGN` refusal: the state directory is checked only by the package's marker",
    "a relative `cdc.checkpoint` under PostgreSQL state is not warned about by rivet",
)


class Capabilities:
    """The version of one rivet binary and the flags its subcommands accept, probed from `--help`."""

    def __init__(self, binary: str) -> None:
        self.binary = binary
        self._help: dict[str, str] = {}
        out = self._run([binary, "--version"])
        match = _VERSION_RE.search(out)
        if not match:
            raise RuntimeError(f"`{binary} --version` did not print a rivet version")
        self.version = tuple(int(g) for g in match.groups())
        self.version_text = ".".join(str(n) for n in self.version)

    @staticmethod
    def _run(argv: list[str]) -> str:
        """Run a probe with a minimal environment and return its stdout."""
        env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
        done = subprocess.run(argv, capture_output=True, text=True, timeout=60, env=env, check=False)
        if done.returncode != 0:
            raise RuntimeError(f"`{' '.join(argv)}` exited {done.returncode}")
        return done.stdout

    def has_flag(self, subcommand: str, flag: str) -> bool:
        """True when `rivet <subcommand> --help` lists the flag."""
        if subcommand not in self._help:
            self._help[subcommand] = self._run([self.binary, subcommand, "--help"])
        return re.search(rf"(?<![\w-]){re.escape(flag)}(?![\w-])", self._help[subcommand]) is not None

    def new_enough(self) -> bool:
        """True when the binary is at or above the minimum the package was written against."""
        return self.version >= MIN_RIVET_VERSION


_CACHE: dict[tuple, Capabilities] = {}


def probe(binary: str) -> Capabilities:
    """Probe a binary once per process and per file identity."""
    stat = os.stat(binary)
    key = (binary, stat.st_mtime_ns, stat.st_size)
    if key not in _CACHE:
        _CACHE[key] = Capabilities(binary)
    return _CACHE[key]
