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
import threading
import time
from contextlib import contextmanager
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
        # Buffered mode: a per-ENGINE sub-ledger under parallel `engine_loop`
        # collects its lines here instead of printing them, so concurrent engines
        # do not interleave into an unreadable stream. The parent `flush_into`s
        # each engine's block in engine order after the join. `None` = print live.
        self._buf: list[str] | None = None
        # Named sub-spans INSIDE a phase — the granularity `phase()` cannot give
        # under the PARALLEL engine matrix, where every engine collapses into one
        # wrapping phase and the buffered children's `_phase_times` are dropped at
        # merge (summing overlapping child phases would overcount). A `span` is a
        # single wall-clock ("mssql: blessed_flow", "postgres: seed"); `flush_into`
        # folds a child's spans into the parent, and `report()` prints them as a
        # SEPARATE breakdown that is honest about the overlap — spans in different
        # engines run concurrently, so they are ranked, never summed into a total.
        self._spans: list[tuple[str, float]] = []

    # ── printing ──
    def _c(self, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self._colour else text

    def _emit(self, line: str) -> None:
        if self._buf is None:
            print(line, flush=True)
        else:
            self._buf.append(line)

    def phase(self, msg: str) -> None:
        # Close the previous phase's wall-clock before opening this one.
        now = time.perf_counter()
        if self._cur_phase is not None:
            self._phase_times.append((self._cur_phase, now - self._phase_start))
        self._cur_phase = msg
        self._phase_start = now
        self._emit(self._c("1;34", f"▸ {msg}"))

    def ok(self, msg: str) -> None:
        self._emit(self._c("1;32", f"  ✓ {msg}"))

    def bad(self, msg: str) -> None:
        self._emit(self._c("1;31", f"  ✗ {msg}"))

    def skip(self, msg: str) -> None:
        self._emit(self._c("1;33", f"  ⊘ {msg}"))

    @contextmanager
    def span(self, name: str):
        """Time a named step inside a phase (e.g. one scenario of one engine).
        Records a single wall-clock into `_spans`; survives the buffered-child
        merge that drops `_phase_times`, so the parallel engine matrix's internals
        are visible in the final timing breakdown. Nesting is fine — a span and a
        sub-span it contains are ranked independently, not netted."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self._spans.append((name, time.perf_counter() - t0))

    def buffered_child(self) -> "Ledger":
        """A sub-ledger that BUFFERS output (for one parallel engine). Its cells +
        buffered lines are folded back with `flush_into` after the engine finishes."""
        child = Ledger(colour=self._colour)
        child._buf = []
        return child

    def flush_into(self, parent: "Ledger") -> None:
        """Fold this buffered child into `parent`: print its collected block (in one
        contiguous run, so an engine's output is not interleaved with a sibling's)
        and merge its cells. Per-phase timings are NOT merged — the parent's
        wrapping phase already holds the parallel wall-clock; summing overlapping
        child phases would overcount."""
        for line in self._buf or []:
            parent._emit(line)
        parent.cells.extend(self.cells)
        # Spans DO travel (unlike _phase_times): each is a per-engine wall-clock
        # the parent reports ranked, not summed, so overlap across engines is not
        # double-counted. This is the only window into the parallel matrix.
        parent._spans.extend(self._spans)

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
        # Inside-the-phase breakdown — the per-engine / per-scenario spans that the
        # opaque "Engine matrix" phase hides. Ranked slowest-first; NOT summed,
        # because spans in different engines overlap under `--engine-parallel`. This
        # is the "where does the time go INSIDE the calls" view.
        if self._spans:
            self.phase("Timing — inside the parallel stages (per span; concurrent, so ranked not summed)")
            for name, dur in sorted(self._spans, key=lambda p: p[1], reverse=True)[:40]:
                print(f"  {dur / 60.0:6.1f} min  {name}")
            print()
            # Per-CELL rollup: a matrix cell span is named "cell <engine> <kind> …".
            # There are too many to list flat, and the distribution — not any one
            # cell — is what decides whether cell-level parallelism would pay. Bucket
            # by "<engine> <kind>" and show count / sum / mean / max / slowest. Sums
            # are within ONE engine's SEQUENTIAL cell loop, so they ARE additive; the
            # gap between a group's SUM and its MAX is exactly the wall-clock a
            # parallel cell loop could reclaim.
            cells = [(n, d) for n, d in self._spans if n.startswith("cell ")]
            if cells:
                groups: dict[str, list[tuple[str, float]]] = {}
                for n, d in cells:
                    key = " ".join(n.split()[1:3])  # "<engine> <kind>"
                    groups.setdefault(key, []).append((n, d))
                self.phase("Timing — matrix cells per engine×kind (SUM is sequential; SUM−MAX = parallelisable slack)")
                for key in sorted(groups, key=lambda k: sum(d for _, d in groups[k]), reverse=True):
                    members = groups[key]
                    tot = sum(d for _, d in members)
                    mx_name, mx = max(members, key=lambda p: p[1])
                    print(f"  {tot / 60.0:6.1f} min  {key:22} n={len(members):<3} "
                          f"mean={tot / len(members):4.1f}s  max={mx:4.1f}s ({mx_name.split(maxsplit=3)[-1]})")
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


# ── matrix cell concurrency ──────────────────────────────────────────────────────
# The engine matrix is I/O-wait-bound (measured: ~62% CPU idle on 12 cores while
# 4 engines run), so its many small, INDEPENDENT cells (own work-dir/prefix each)
# can run concurrently to fill the idle cores. Engines ALSO run in parallel, so a
# per-engine cell pool alone would oversubscribe (engines × pool); this ONE global
# semaphore, shared by every engine's cell pool, caps TOTAL in-flight cells so the
# shared state DB and source containers are not stampeded. Tune with --cell-parallel.
_cell_parallel_n = 8
_cell_gate = threading.BoundedSemaphore(_cell_parallel_n)


def set_cell_parallel(n: int) -> None:
    """Resize the global cell-concurrency cap (called once from main after arg parse)."""
    global _cell_gate, _cell_parallel_n
    _cell_parallel_n = max(1, n)
    _cell_gate = threading.BoundedSemaphore(_cell_parallel_n)


def cell_parallel() -> int:
    """The configured cap value — for sizing a per-engine pool (the semaphore is the
    real global limiter; this just avoids spawning far more threads than can ever run)."""
    return _cell_parallel_n


def cell_gate() -> threading.BoundedSemaphore:
    """The shared limiter. Use as `with cell_gate(): run_cell(...)`. Read via a
    function, not a captured value, so `set_cell_parallel` is honoured after import."""
    return _cell_gate


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
