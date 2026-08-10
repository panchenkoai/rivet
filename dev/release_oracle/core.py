"""Shared plumbing for the release oracle: the result ledger, process running,
and the store/engine helpers every layer needs.

Why this exists in Python at all — the bash it replaces was correct in shape but
kept losing to the SHELL rather than to the checks it makes. Three bites, all in
`dev/release-oracle/`:

* `local store=$1 ... dl="…${store}…"` on ONE line. macOS bash 3.2 expands the
  same-line `${store}` against the ENCLOSING scope, so it silently took the
  caller's value where one existed (the batch load path, which has a `store`
  local) and, under `set -u`, ABORTED the function where none did (the CDC
  path). The CDC layer of the go/no-go gate therefore reported
  `independent-readback[!=5]` for every engine and could never pass.
* the same gotcha twice more in `run.sh` (`bring_up`, `seed_engine`), each fixed
  with a "# own line (bash 3.2 …)" comment — a fix that has to be remembered per
  site rather than made impossible.
* `$?` after a pipeline reading the exit status of the LAST stage, so a failing
  check read as a pass.

None of those failure modes exist here: names are function-local by
construction, a subprocess result is an object with its own returncode, and
strings are passed as argv lists rather than re-parsed by a shell.

The output format is deliberately IDENTICAL to the bash version — same glyphs,
same colours, same final table and exit code — so a reader (or a CI log diff)
cannot tell which implementation produced a run, and the rewrite can be verified
by comparing transcripts rather than by trust.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


class Status(str, Enum):
    """A cell's outcome.

    `SKIP` is load-bearing: a down service or an absent credential must never
    read as a pass. Only `FAIL` sets the non-zero exit.
    """

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass(frozen=True)
class Cell:
    engine: str
    version: str
    scenario: str
    store: str
    status: Status
    detail: str = ""


class Ledger:
    """Every check's outcome, and the one place that decides releasability.

    The bash version kept a `RESULTS` array of `|`-joined strings plus a separate
    `RED` flag mutated by `bad()`. Two representations of one fact drift: a check
    could print `✗` without adding a row, or add a FAIL row without setting RED
    (the exit code then said releasable). Here the verdict is DERIVED from the
    rows, so the printed table and the exit code cannot disagree.
    """

    def __init__(self, *, colour: bool | None = None) -> None:
        self.cells: list[Cell] = []
        if colour is None:
            colour = sys.stdout.isatty() or os.environ.get("FORCE_COLOR") == "1"
        self._colour = colour
        # Per-phase wall-clock, so the gate self-reports WHERE the 30–60 min goes
        # (each `phase()` closes the previous one; `report()` prints the breakdown
        # sorted slowest-first). Answers "what takes so long" every run, cheaply.
        self._phase_times: list[tuple[str, float]] = []
        self._cur_phase: str | None = None
        self._phase_start: float = time.perf_counter()

    # ── printing ──
    def _c(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self._colour else text

    def phase(self, msg: str) -> None:
        # Close the previous phase's wall-clock before opening this one.
        now = time.perf_counter()
        if self._cur_phase is not None:
            self._phase_times.append((self._cur_phase, now - self._phase_start))
        self._cur_phase = msg
        self._phase_start = now
        print(self._c("1;34", f"▸ {msg}"), flush=True)

    def ok(self, msg: str) -> None:
        print(self._c("1;32", f"  ✓ {msg}"), flush=True)

    def bad(self, msg: str) -> None:
        print(self._c("1;31", f"  ✗ {msg}"), flush=True)

    def skip(self, msg: str) -> None:
        print(self._c("1;33", f"  ⊘ {msg}"), flush=True)

    # ── recording ──
    def add(
        self,
        engine: str,
        version: str,
        scenario: str,
        store: str,
        status: Status,
        detail: str = "",
    ) -> None:
        self.cells.append(Cell(engine, version, scenario, store, status, detail))

    def passed(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        """Print the ✓ AND record the row — one call, so the two cannot diverge."""
        self.ok(msg)
        self.add(engine, version, scenario, store, Status.PASS, detail or msg)

    def failed(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        self.bad(msg)
        self.add(engine, version, scenario, store, Status.FAIL, detail or msg)

    def skipped(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        self.skip(msg)
        self.add(engine, version, scenario, store, Status.SKIP, detail or msg)

    # ── verdict ──
    @property
    def red(self) -> bool:
        return any(c.status is Status.FAIL for c in self.cells)

    def report(self) -> int:
        print()
        self.phase("Release Oracle result")
        print(f"  {'ENGINE':<10} {'VER':<6} {'SCENARIO':<16} {'STORE':<8} STATUS")
        for c in self.cells:
            print(
                f"  {c.engine:<10} {c.version:<6} {c.scenario:<16} {c.store:<8} "
                f"{c.status.value} {c.detail}"
            )
        print()
        # Wall-clock breakdown — the phases that actually cost the 30–60 min,
        # slowest first. `phase("Release Oracle result")` above closed the last
        # real phase, so `_phase_times` now holds every phase but this report line.
        timed = [t for t in self._phase_times if not t[0].startswith("Release Oracle result")]
        if timed:
            total = sum(d for _, d in timed)
            self.phase("Timing (wall-clock, slowest first)")
            for name, dur in sorted(timed, key=lambda p: p[1], reverse=True)[:15]:
                pct = (dur / total * 100.0) if total > 0 else 0.0
                print(f"  {dur / 60.0:6.1f} min  {pct:4.0f}%  {name}")
            print(f"  {total / 60.0:6.1f} min  total (sum of phases)")
            print()
        if self.red:
            print(self._c("1;31", "  NOT RELEASABLE — one or more cells failed (see ✗ above)."))
            return 1
        print(self._c("1;32", "  RELEASE-READY — every non-skipped cell is green."))
        return 0


# ── process running ────────────────────────────────────────────────────────────
@dataclass
class Proc:
    """A finished process. `ok` is the EXIT STATUS, never a grep over the output.

    The bash version decided some checks with `grep -qaiE "error|failed"` over
    combined output, which fires on a row whose DATA contains the word "error"
    and misses a silent non-zero exit. Both facts are kept separate here.
    """

    argv: Sequence[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def out(self) -> str:
        """stdout+stderr, for the cases that genuinely want the transcript."""
        return self.stdout + self.stderr


def run(
    argv: Sequence[str],
    *,
    stdin: str | None = None,
    timeout: float | None = 600,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> Proc:
    """Run `argv` (a LIST — no shell, so no quoting or word-splitting bugs)."""
    full_env = {**os.environ, **(env or {})}
    try:
        p = subprocess.run(
            list(argv),
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=full_env,
            cwd=str(cwd) if cwd else None,
        )
        return Proc(argv, p.returncode, p.stdout, p.stderr)
    except subprocess.TimeoutExpired as e:
        out = e.stdout or ""
        err = (e.stderr or "") + f"\n[timeout after {timeout}s]"
        return Proc(argv, 124, out if isinstance(out, str) else out.decode(), err)
    except FileNotFoundError as e:
        return Proc(argv, 127, "", str(e))


def docker(*args: str, **kw) -> Proc:
    return run(["docker", *args], **kw)


def docker_exec(container: str, *args: str, stdin: str | None = None, **kw) -> Proc:
    flags = ["-i"] if stdin is not None else []
    return docker("exec", *flags, container, *args, stdin=stdin, **kw)


def have(tool: str) -> bool:
    return shutil.which(tool) is not None


def wait_until(check, *, tries: int = 45, delay: float = 2.0) -> bool:
    """Poll `check()` until true. Returns False on exhaustion — never raises, so
    a caller records a SKIP instead of aborting the whole gate."""
    for _ in range(tries):
        if check():
            return True
        time.sleep(delay)
    return False


# ── the release binary under test ──────────────────────────────────────────────
def rivet_bin() -> Path:
    return Path(os.environ.get("RIVET_BIN", ROOT / "target" / "release" / "rivet"))


def rivet(*args: str, **kw) -> Proc:
    return run([str(rivet_bin()), *args], **kw)


# ── containers this gate owns ──────────────────────────────────────────────────
ENGINE_PREFIX = "rivet-oracle-eng-"


def engine_container(engine: str, tag: str) -> str:
    """The container name for one engine×version.

    A plain function of its arguments — the bash equivalent had to be split onto
    its own line because a same-line `${eng}` read the enclosing scope, which is
    how the BigQuery stage once named its container after whichever engine the
    main loop had last visited.
    """
    return f"{ENGINE_PREFIX}{engine}-{tag.replace('.', '_')}"


def remove_engine_containers() -> None:
    ps = docker("ps", "-aq", "--filter", f"name={ENGINE_PREFIX}")
    for cid in ps.stdout.split():
        docker("rm", "-fv", cid)


def container_for_port(port: int) -> str | None:
    """The running container publishing `port` — how the CDC layer finds the
    engine behind a URL. Returns None rather than an empty string, so a caller
    cannot pass ""/None into `docker exec` and get "invalid container name or ID:
    value is empty" (which is exactly what the bash version did)."""
    for name in docker("ps", "--format", "{{.Names}}").stdout.split():
        if any(line.endswith(f":{port}") for line in docker("port", name).stdout.splitlines()):
            return name
    return None


def port_of(url: str) -> int | None:
    """The TCP port in a connection URL, or None."""
    import re

    m = re.search(r":(\d+)(?:/|$)", url)
    return int(m.group(1)) if m else None
