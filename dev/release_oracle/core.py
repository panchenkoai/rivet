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

import functools

import os
import re
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
    read as a pass. `FAIL` and `INFRA` set the non-zero exit; only `FAIL` says the product is wrong.
    """

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    KNOWN = "KNOWN"  # a failure recorded in known_red.py with a reason, in date
    INFRA = "INFRA"  # the ORACLE could not read (its own transport), so the cell graded nothing


@dataclass(frozen=True)
class Cell:
    engine: str
    version: str
    scenario: str
    store: str
    status: Status
    detail: str = ""


#: Stage function name -> ledger rows it recorded this run (filled by `record_stage_runs` wrappers).
STAGES_RUN: dict[str, int] = {}
#: Every `sc_*` / `verify_*` name `record_stage_runs` found in the gate's modules.
STAGES_DEFINED: set[str] = set()
_STAGES_LOCK = threading.Lock()


def record_stage_runs(modules: Iterable[object]) -> None:
    """Wrap every `sc_*` / `verify_*` bound in `modules` so a call records its name and the rows it added, and a raise becomes a FAIL row instead of ending the gate."""
    for mod in modules:
        for name, fn in list(vars(mod).items()):
            if not (name.startswith(("sc_", "verify_")) and callable(fn)):
                continue
            STAGES_DEFINED.add(name)
            if getattr(fn, "_stage_recorded", False):
                continue

            def wrapped(*args, _name=name, _fn=fn, **kw):
                led = args[0] if args and isinstance(args[0], Ledger) else None
                before = len(led.cells) if led else 0
                try:
                    return _fn(*args, **kw)
                except (Exception, SystemExit) as e:
                    if led is None:
                        raise
                    import traceback
                    traceback.print_exc()
                    eng = args[1] if len(args) > 1 and isinstance(args[1], str) else "-"
                    first = next((ln for ln in str(e).splitlines() if ln.strip()), "")
                    led.failed(eng, "-", _name, "-", f"{_name} raised {type(e).__name__}: {first}")
                    return None
                finally:
                    added = len(led.cells) - before if led else 0
                    with _STAGES_LOCK:
                        STAGES_RUN[_name] = STAGES_RUN.get(_name, 0) + added

            wrapped._stage_recorded = True  # type: ignore[attr-defined]
            wrapped.__name__ = name
            setattr(mod, name, wrapped)


def matrix_test_stages(doc: dict, defined: set[str]) -> dict[str, str]:
    """The `test` rows of the gate matrix -> the stage each must run: `verify_<id>` for a preflight (and an infra row that has one), `sc_<id>` for a scenario with any `test` cell."""
    out: dict[str, str] = {}
    for row in doc.get("preflights") or []:
        if row.get("status") == "test":
            out[f"verify_{row['id']}"] = f"preflight `{row['id']}`"
    for row in doc.get("infra") or []:
        if row.get("status") == "test" and f"verify_{row['id']}" in defined:
            out[f"verify_{row['id']}"] = f"infra `{row['id']}`"
    for row in doc.get("scenarios") or []:
        if any(v == "test" for k, v in row.items() if k not in ("id", "what")):
            out[f"sc_{row['id']}"] = f"scenario `{row['id']}`"
    return out


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
        self.known_seen: set[str] = set()
        self.known_passed: set[str] = set()
        if colour is None:
            colour = sys.stdout.isatty() or os.environ.get("FORCE_COLOR") == "1"
        self._colour = colour
        # Per-phase wall-clock, so the gate self-reports WHERE the 30–60 min goes
        # (each `phase()` closes the previous one; `report()` prints the breakdown
        # sorted slowest-first). Answers "what takes so long" every run, cheaply.
        self._phase_times: list[tuple[str, float]] = []
        #: Phase -> the machine's 1-minute load average when it opened and when it closed.
        self._phase_load: dict[str, tuple[float, float]] = {}
        self._load_at_start = 0.0
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
        now, load = time.perf_counter(), os.getloadavg()[0]
        if self._cur_phase is not None:
            self._phase_times.append((self._cur_phase, now - self._phase_start))
            self._phase_load[self._cur_phase] = (self._load_at_start, load)
        self._load_at_start = load
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

    def record_span(self, name: str, seconds: float) -> None:
        """Record a wall-clock measured by the caller (a step between two marks)."""
        self._spans.append((name, seconds))

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
        parent.known_seen |= self.known_seen
        parent.known_passed |= self.known_passed
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
        from . import known_red
        entry, _ = known_red.match(msg)
        if entry is not None:
            self.known_passed.add(entry.match)
        self.ok(msg)
        self.add(engine, version, scenario, store, Status.PASS, detail or msg)

    def failed(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        from . import known_red
        entry, in_date = known_red.match(msg)
        if entry is not None:
            self.known_seen.add(entry.match)
        if entry is not None and in_date:
            self.skip(f"KNOWN RED (until {entry.expires}: {entry.reason}) — {msg}")
            self.add(engine, version, scenario, store, Status.KNOWN, detail or msg)
            return
        if entry is not None:
            msg = f"{msg} — its known-red entry EXPIRED {entry.expires}: fix it or renew the entry"
        self.bad(msg)
        self.add(engine, version, scenario, store, Status.FAIL, detail or msg)

    def cell_passed(self, cell: str) -> None:
        """A passing live cell: the known-red entry written for its failure (`<cell> — <symptom>`) is fixed, not unexercised."""
        from . import known_red
        self.known_passed.update(k.match for k in known_red.KNOWN_RED if k.match.startswith(f"{cell} — "))

    def close_known_red(self) -> None:
        """After a FULL run: a known-red entry no failure matched is fixed — it must be removed."""
        from . import known_red
        for k in known_red.KNOWN_RED:
            if k.match not in self.known_seen:
                # Straight to FAIL: the message quotes the entry, so `failed` would match it.
                why = ("its cell PASSED this run — it is fixed"
                       if k.match in self.known_passed else
                       "no cell this run exercised it, so it excuses nothing")
                msg = (f"known-red entry `{k.match}` matched no failure: {why}; "
                       "remove it from dev/release_oracle/known_red.py")
                self.bad(msg)
                self.add("-", "-", "known_red", "-", Status.FAIL, msg)

    def close_matrix_rows(self, doc: dict | None = None) -> None:
        """After a FULL run: every `test` row of docs/release-gate-matrix.yaml ran its stage, and the stage recorded a row."""
        if doc is None:
            import yaml
            doc = yaml.safe_load((ROOT / "docs/release-gate-matrix.yaml").read_text())
        for fn, row in sorted(matrix_test_stages(doc, STAGES_DEFINED).items()):
            rows = STAGES_RUN.get(fn)
            if rows is None:
                why = f"{row} is `test` in docs/release-gate-matrix.yaml but {fn}() never ran this gate"
            elif rows == 0:
                why = f"{row} ran {fn}() and it recorded no ledger row: a check that graded nothing"
            else:
                continue
            self.bad(why)
            self.add("-", "-", "matrix_rows", "-", Status.FAIL, why)

    def ungraded(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        """The oracle's own infrastructure failed: blocking, and never a verdict on the product."""
        self._emit(self._c("1;35", f"  ⚠ ORACLE INFRA (the cell graded nothing; re-run it) — {msg}"))
        self.add(engine, version, scenario, store, Status.INFRA, detail or msg)

    def skipped(self, engine: str, version: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
        self.skip(msg)
        self.add(engine, version, scenario, store, Status.SKIP, detail or msg)

    # ── verdict ──
    @property
    def red(self) -> bool:
        return any(c.status is Status.FAIL for c in self.cells)

    @property
    def ungraded_cells(self) -> int:
        return sum(c.status is Status.INFRA for c in self.cells)

    def verdict(self) -> str:
        """The run's verdict, derived from the rows: a product FAIL outranks an ungraded cell."""
        return "NOT RELEASABLE" if self.red else "NOT GRADED" if self.ungraded_cells else "RELEASE-READY"

    def report(self, *, full: bool = False) -> int:
        """Print the table and the verdict; only a `full` gate run records a timings line and may say RELEASE-READY."""
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
                lo, hi = self._phase_load.get(name, (0.0, 0.0))
                print(f"  {dur / 60.0:6.1f} min  {pct:4.0f}%  load {lo:5.1f}->{hi:5.1f}  {name}")
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
            # Per-STEP rollup across every cell: "step <engine> <stage> <store>"
            # spans, grouped by stage × store — which step of the chain the
            # matrix's time actually goes to.
            steps = [(n, d) for n, d in self._spans if n.startswith("step ")]
            if steps:
                by: dict[str, list[float]] = {}
                for n, d in steps:
                    _, _eng, stage, store = n.split(maxsplit=3)
                    by.setdefault(f"{stage} {store}", []).append(d)
                self.phase("Timing — chain steps per stage×store (summed over every cell and engine)")
                for key in sorted(by, key=lambda k: sum(by[k]), reverse=True):
                    ds = by[key]
                    print(f"  {sum(ds) / 60.0:6.1f} min  {key:28} n={len(ds):<4} "
                          f"mean={sum(ds) / len(ds):5.1f}s  max={max(ds):5.1f}s")
                print()
        verdict = self.verdict()
        if full:
            record_timings(
                TIMINGS_HISTORY, timed, self._spans, verdict,
                sum(c.status is Status.PASS for c in self.cells),
                sum(c.status is Status.FAIL for c in self.cells),
                self._phase_load,
            )
        known = [c for c in self.cells if c.status is Status.KNOWN]
        if known:
            print(self._c("1;33", f"  {len(known)} KNOWN RED cell(s), each recorded in known_red.py with a reason and an expiry."))
        if self.ungraded_cells:
            print(self._c("1;35", f"  {self.ungraded_cells} cell(s) NOT GRADED: the oracle's own read failed (see ⚠ above). "
                                  "Not a product verdict; re-run them."))
        if self.red:
            print(self._c("1;31", "  NOT RELEASABLE — one or more cells failed (see ✗ above)."))
            return 1
        if self.ungraded_cells:
            print(self._c("1;35", "  NOT GRADED — no cell failed, and the cells above were never graded."))
            return 1
        if not full:
            print(self._c("1;33", f"  PARTIAL RUN — the {len(self.cells)} row(s) selected carry no failure. Not a gate "
                                  "verdict, and no timings line was written."))
            return 0
        print(self._c("1;32", "  RELEASE-READY — every non-skipped cell is green."))
        return 0


def first_error(text: str) -> str:
    """The first line of a command's output that names an error, else its last non-empty line."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    return next((ln for ln in lines if re.match(r"(?i)(error\b|\[RIVET_|.*\berror:)", ln)), lines[-1] if lines else "")


#: One JSON line per full gate run (gitignored): where the wall-clock went, so a slow run is compared, not guessed.
TIMINGS_HISTORY = ROOT / "dev" / "release-oracle" / "timings.jsonl"


def record_timings(path: Path, phases: list[tuple[str, float]], spans: list[tuple[str, float]],
                   verdict: str, passed: int, failed: int, loads: dict[str, tuple[float, float]] | None = None) -> dict:
    """Append this run's phase and span timings, and each phase's 1-minute load average at its start and end, to `path`; print the phase deltas against the previous run."""
    import datetime as _dt
    import json as _json

    head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                          capture_output=True, text=True).stdout.strip()
    rec = {
        "at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "commit": head,
        "verdict": verdict,
        "passed": passed,
        "failed": failed,
        "total_min": round(sum(d for _, d in phases) / 60.0, 1),
        "phases_min": {n.split(" — ")[0]: round(d / 60.0, 2) for n, d in phases},
        "phases_load1": {n.split(" — ")[0]: [round(x, 1) for x in v] for n, v in (loads or {}).items()},
        "cores": os.cpu_count(),
        "top_spans_min": {n: round(d / 60.0, 2) for n, d in sorted(spans, key=lambda p: p[1], reverse=True)[:40]},
    }
    prev = None
    if path.exists():
        lines = [x for x in path.read_text().splitlines() if x.strip()]
        prev = _json.loads(lines[-1]) if lines else None
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(_json.dumps(rec) + "\n")
    if prev:
        print(f"  vs previous run ({prev.get('commit')} {prev.get('at')}): total "
              f"{prev.get('total_min')} -> {rec['total_min']} min")
        before = prev.get("phases_min", {})
        deltas = sorted(((rec["phases_min"].get(n, 0.0) - before.get(n, 0.0), n)
                         for n in set(before) | set(rec["phases_min"])), reverse=True)
        for d, n in [x for x in deltas if abs(x[0]) >= 0.5][:10]:
            print(f"    {d:+6.1f} min  {n}")
    print(f"  timings appended to {path}")
    return rec


# ── process running ────────────────────────────────────────────────────────────
# rivet's release binary only WARNS when a run skipped a per-export facade (debug
# builds panic); every command the gate runs is scanned for it, and the gate fails on it.
INVARIANT_MARKER = "run-integrity invariant violated"
INVARIANT_HITS: list[tuple[str, str]] = []


#: rivet's INTERNAL errors (a broken invariant — a bug) carry a `RIVET_INTERNAL_*` code, in the
#: `[CODE]` text prefix and the JSON `code` field alike. One in ANY gated command fails the gate,
#: even where the test expected the command to fail.
INTERNAL_MARKER = "RIVET_INTERNAL_"
INTERNAL_HITS: list[tuple[str, str]] = []


def note_invariant_violations(argv: Sequence[str], text: str) -> None:
    """Record each run-integrity invariant warning and each INTERNAL error in a command's output."""
    for line in text.splitlines():
        cmd = " ".join(str(a) for a in argv)[:200]
        if INVARIANT_MARKER in line:
            INVARIANT_HITS.append((cmd, line.strip()[:400]))
        if INTERNAL_MARKER in line and ("Error" in line or '"code"' in line):
            INTERNAL_HITS.append((cmd, line.strip()[:400]))


def verify_no_invariant_violations(led: "Ledger") -> None:
    """Fail the gate for every run that reported an incomplete integrity record."""
    led.phase("Run-integrity invariant — no gated run may skip a per-export facade")
    for cmd, line in INTERNAL_HITS:
        led.failed("all", "-", "internal-error", "-",
                   f"internal error (a rivet bug) in `{cmd}` — {line}", "internal")
    if not INVARIANT_HITS:
        led.passed("all", "-", "run-integrity", "-",
                   "run-integrity: no gated run reported a skipped facade", "clean")
        return
    for cmd, line in INVARIANT_HITS:
        led.failed("all", "-", "run-integrity", "-", f"run-integrity: `{cmd}` — {line}", "violated")


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

    def __post_init__(self) -> None:
        note_invariant_violations(self.argv, self.stdout + self.stderr)

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def out(self) -> str:
        """stdout+stderr, for the cases that genuinely want the transcript."""
        return self.stdout + self.stderr

    @property
    def why(self) -> str:
        """`exit N [RIVET_CODE]: <first error line>` for a ledger row; the whole stderr goes to the gate's stderr."""
        err = (self.stderr or self.stdout or "").strip()
        if err:
            print(f"── {' '.join(map(str, self.argv))[-200:]} (exit {self.returncode}) ──\n{err}", file=sys.stderr, flush=True)
        code = re.search(r"\[(RIVET_[A-Z0-9_]+)\]", err)
        return f"exit {self.returncode}{f' [{code.group(1)}]' if code else ''}: {first_error(err)[:300]}"


#: Every RIVET_* variable the gate, the Rust harness or the Makefile's GATE_ENV READS from the
#: shell. Any other RIVET_* in the inherited environment is the PRODUCT's (a fault hook, a state
#: URL, a tuning knob left over from a manual repro) and is dropped before the first cell: `run()`
#: merges os.environ into every child, so a leftover looked like a product failure in every cell.
#: Enumerated from the code, graded by tests/offline/harness_env_hygiene_guard.rs.
HARNESS_ENV: frozenset[str] = frozenset((
    "RIVET_ALLOW_WORKTREE_LIVE", "RIVET_BIN", "RIVET_BIN_OVERRIDE",
    "RIVET_CDC_STATE_URL", "RIVET_CONC_SRC_CONTAINER", "RIVET_CONC_SRC_URL", "RIVET_CONC_STATE_URL",
    "RIVET_FAILURE_MONGO_URL", "RIVET_FIELD_LOCK", "RIVET_FIELD_MYSQL_CONTAINER", "RIVET_FIELD_REPLAY_BIN",
    "RIVET_FLOW_VERDICTS", "RIVET_GATE_SHARED_STATE", "RIVET_GATE_STATE_URL",
    "RIVET_HARM_SLACK", "RIVET_HARM_TOL",
    "RIVET_ORACLE_DOCKER", "RIVET_ORACLE_LATEST_ONLY", "RIVET_ORACLE_LOG", "RIVET_ORACLE_SELFTEST_FLAG",
    "RIVET_ORACLE_VERSIONS", "RIVET_ORACLE_WITHOUT_PREV_RELEASE", "RIVET_ORACLE_WORK",
    "RIVET_PERF_BQ_WALL_TOL", "RIVET_PERF_CPU_TOL", "RIVET_PERF_RSS_TOL", "RIVET_PERF_WALL_TOL",
    "RIVET_PREV_RELEASE_BIN", "RIVET_REGENERATE_FIXTURES", "RIVET_REGRESSION_SOURCE_URL",
    "RIVET_REGRESSION_WALL_TOL", "RIVET_SCALE_CHUNK", "RIVET_SCALE_RSS_TOL",
    "RIVET_SF_CONNECTION", "RIVET_SF_DATABASE", "RIVET_SF_SCHEMA", "RIVET_SF_STORAGE_INTEGRATION",
    "RIVET_SF_WAREHOUSE", "RIVET_SKIP_LOG", "RIVET_SNOWFLAKE_KEY",
    "RIVET_SOAK_BYTE_CAP", "RIVET_SOAK_ROLLOVER", "RIVET_SOAK_ROLLOVER_MB", "RIVET_SWEEP_STATE_CONTAINER",
    "RIVET_TEST_BQ_DATASET", "RIVET_TEST_EXCLUSIVE", "RIVET_TEST_GCS_BUCKET", "RIVET_TEST_ORACLE_LATIN1_URL",
    "RIVET_TEST_STATE_TOXI_URL", "RIVET_TEST_STATE_URL", "RIVET_TINYFS_DIR",
    "RIVET_UPG_KEEP", "RIVET_UPG_ORACLE_CDC_URL",
))
#: The per-engine URL families (`RIVET_CDC_<ENGINE>_URL`, `RIVET_ORACLE_<ENGINE>_URL`), read by name.
HARNESS_ENV_FAMILY = re.compile(r"^RIVET_(CDC|ORACLE)_[A-Z0-9]+_URL$")


def is_harness_env(name: str) -> bool:
    """Is `name` a RIVET_* variable the harness itself reads (so the shell may hand it in)?"""
    return name in HARNESS_ENV or bool(HARNESS_ENV_FAMILY.match(name))


def scrub_inherited_rivet_env() -> list[str]:
    """Drop every inherited RIVET_* that is not the harness's own; return the dropped names."""
    dropped = sorted(k for k in os.environ if k.startswith("RIVET_") and not is_harness_env(k))
    for k in dropped:
        del os.environ[k]
    return dropped


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
        # TimeoutExpired carries raw bytes even under `text=True`; decode both streams.
        def _text(v: "bytes | str | None") -> str:
            if v is None:
                return ""
            return v if isinstance(v, str) else v.decode(errors="replace")

        return Proc(argv, 124, _text(e.stdout), _text(e.stderr) + f"\n[timeout after {timeout}s]")
    except FileNotFoundError as e:
        return Proc(argv, 127, "", str(e))


def docker(*args: str, **kw) -> Proc:
    return run(["docker", *args], **kw)


def docker_exec(container: str, *args: str, stdin: str | None = None, **kw) -> Proc:
    flags = ["-i"] if stdin is not None else []
    return docker("exec", *flags, container, *args, stdin=stdin, **kw)


def have(tool: str) -> bool:
    """Is `tool` usable — for `duckdb`, the uv-pinned package the harness runs (dev/pytools/duckcli.py), never a binary on PATH."""
    if tool == "duckdb":
        import importlib.util

        return importlib.util.find_spec("duckdb") is not None
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


def server_of(url: str) -> str:
    """`host:port` of a connection URL: two cells with the same value share one server (localhost is 127.0.0.1)."""
    import urllib.parse

    u = urllib.parse.urlsplit(url)
    host = (u.hostname or "").lower()
    return f"{'127.0.0.1' if host == 'localhost' else host}:{u.port}"


def run_lanes(led: "Ledger", cells: Sequence[tuple[object, Callable[["Ledger"], None]]], *,
              workers: int | None = None) -> None:
    """Run `(lane, fn)` cells: one lane's cells in list order, lanes side by side; every cell's rows are buffered and flushed in list order."""
    from concurrent.futures import ThreadPoolExecutor

    subs = [led.buffered_child() for _ in cells]
    lanes: dict[object, list[int]] = {}
    for i, (lane, _) in enumerate(cells):
        lanes.setdefault(lane, []).append(i)

    def _lane(idx: list[int]) -> BaseException | None:
        for i in idx:
            try:
                cells[i][1](subs[i])
            except (Exception, SystemExit) as e:  # noqa: BLE001 — re-raised below, after every row is flushed
                return e
        return None

    with ThreadPoolExecutor(max_workers=max(1, min(workers or len(lanes), len(lanes)))) as ex:
        raised = [e for e in ex.map(_lane, lanes.values()) if e is not None]
    for sub in subs:
        sub.flush_into(led)
    if raised:
        raise raised[0]


def wait_until(check, *, tries: int = 45, delay: float = 2.0) -> bool:
    """Poll `check()` until true. Returns False on exhaustion — never raises, so
    a caller records a SKIP instead of aborting the whole gate."""
    for _ in range(tries):
        if check():
            return True
        time.sleep(delay)
    return False


# ── the release binary under test ──────────────────────────────────────────────
def target_dir() -> Path:
    """Cargo's target directory: $CARGO_TARGET_DIR (relative to the repo) when set."""
    return ROOT / os.environ.get("CARGO_TARGET_DIR", "target")


#: live_suite tests (bare fn names) and modules a dedicated cell has already run this gate;
#: the derived `live_modules` cell runs everything else.
RAN_LIVE_TESTS: set[str] = set()
RAN_LIVE_MODULES: set[str] = set()

#: Live tests allowed to SELF-SKIP in a gate run (`module::fn` → why). Any other self-skip
#: fails its cell: libtest counts a skip green, so an unlisted one is a row that graded nothing.
SKIP_ALLOWED: dict[str, str] = {
    "live_cdc::regenerate_the_pgoutput_fixture_from_the_rig_scenarios":
        "a fixture GENERATOR (RIVET_REGENERATE_FIXTURES=1), not a check",
    "live_cdc_mbt::cdc_destination_disk_full_is_loud_and_lossless":
        "needs a mounted tiny filesystem (RIVET_TINYFS_DIR) the stand does not provision",
    "live_keyset_parallel::parallel_keyset_incremental_survives_no_backslash_escapes_mysql":
        "sets @@global.sql_mode, which needs SUPER; the stand's test user has none",
}


def self_skipped(skip_log: Path) -> dict[str, str]:
    """The tests that wrote `RIVET-SKIP <module::fn> — <why>` to `skip_log`."""
    if not skip_log.exists():
        return {}
    # Split on the marker, not on lines: records written by parallel tests can interleave.
    out: dict[str, str] = {}
    for rec in skip_log.read_text().split("RIVET-SKIP ")[1:]:
        m = re.match(r"(\S+) — (.*)", rec.strip(), re.S)
        if m:
            out[m.group(1)] = m.group(2).strip()
    return out


def rivet_bin() -> Path:
    """$RIVET_BIN, else the release binary cargo builds under `target_dir()`."""
    return Path(os.environ.get("RIVET_BIN", target_dir() / "release" / "rivet"))


def release_bin_env() -> dict[str, str]:
    """Env that makes a `cargo nextest` leg drive the gate's release binary: the Rust
    tests resolve rivet from RIVET_BIN_OVERRIDE (tests/common/runner.rs), not RIVET_BIN."""
    return {"RIVET_BIN": str(rivet_bin()), "RIVET_BIN_OVERRIDE": str(rivet_bin())}


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


def isolate_state_db(url: str, tag: str) -> str | None:
    """Create a fresh database beside `url`'s and return a URL to it (dropped at exit), or None.

    One gate run gets its own state DB: a build that bumps the state schema must not migrate
    the shared stand DB every other branch and session on this machine still opens."""
    import atexit
    import urllib.parse
    u = urllib.parse.urlsplit(url)
    c = container_for_port(u.port or 5432)
    if c is None or not u.username:
        return None
    db = f"rivet_state_gate_{tag}"
    psql = ["psql", "-U", urllib.parse.unquote(u.username), "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c"]
    if not docker_exec(c, *psql, f"DROP DATABASE IF EXISTS {db}").ok or \
            not docker_exec(c, *psql, f"CREATE DATABASE {db}").ok:
        return None
    atexit.register(lambda: docker_exec(c, *psql, f"DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
    return urllib.parse.urlunsplit((u.scheme, u.netloc, f"/{db}", u.query, u.fragment))


def state_db_name() -> str:
    """The gate's Postgres state database (the per-run one when isolated)."""
    import urllib.parse
    url = os.environ.get("RIVET_GATE_STATE_URL", "")
    return urllib.parse.urlsplit(url).path.lstrip("/") or "rivet_state"


def container_for_port(port: int) -> str | None:
    """The running container publishing `port` — how the CDC layer finds the
    engine behind a URL. Returns None rather than an empty string, so a caller
    cannot pass ""/None into `docker exec` and get "invalid container name or ID:
    value is empty" (which is exactly what the bash version did)."""
    names = docker("ps", "--filter", f"publish={port}", "--format", "{{.Names}}").stdout.split()
    return names[0] if names else None


def port_of(url: str) -> int | None:
    """The TCP port in a connection URL, or None."""
    import re

    m = re.search(r":(\d+)(?:/|$)", url)
    return int(m.group(1)) if m else None


@functools.cache
def sqlcmd(container: str) -> tuple[str, ...]:
    """sqlcmd for THIS SQL Server image: tools18 (2022, needs `-C`) or tools (2019, no `-C`)."""
    for path, flags in (("/opt/mssql-tools18/bin/sqlcmd", ("-C",)), ("/opt/mssql-tools/bin/sqlcmd", ())):
        if docker_exec(container, "test", "-x", path, timeout=20).ok:
            return (path, *flags)
    raise SystemExit(f"{container}: no sqlcmd at tools18 or tools — the image changed its layout")


# A nextest status line: `<STATUS> [ 1.2s] (3/7) <binary> <test>`. The status is the whole
# run of capitals before `[`, so `FAIL + LEAK` is read whole: matching `(PASS|LEAK) [`
# found the `LEAK [` inside it and graded a failed, leaky test green.
_NEXTEST_LINE = re.compile(r"^\s*([A-Z][A-Z +]*[A-Z])\s+\[[^\]]*\] \([^)]*\) \S+ (\S+)", re.M)


def nextest_outcomes(out: str) -> dict[str, str]:
    """Each test's final nextest status (`PASS`, `LEAK`, `FAIL`, `FAIL + LEAK`, …); `SLOW` is not final."""
    final: dict[str, str] = {}
    for status, name in _NEXTEST_LINE.findall(out):
        if status != "SLOW":
            final[name] = status
    return final


def nextest_started(out: str) -> int | None:
    """How many tests nextest said it would run (`Starting N tests`), or None if it never started."""
    m = re.search(r"Starting (\d+) tests?\b", out)
    return int(m.group(1)) if m else None


_NEXTEST_PANIC = re.compile(r"^\s*thread '([^']+)'(?: \(\d+\))? panicked at [^\n]*\n[ \t]*([^\n]*)(?:\n[ \t]+- ([^\n]*))?", re.M)


def nextest_panics(out: str) -> dict[str, str]:
    """Each test's first panic as one line: the message's first line, or the rig oracle's first finding (its header names a per-run export)."""
    first: dict[str, str] = {}
    for name, head, finding in _NEXTEST_PANIC.findall(out):
        first.setdefault(name, (f"rig oracle: {finding}" if head.startswith("rig oracle: ") and finding else head).strip()[:300])
    return first


def nextest_passed(out: str) -> set[str]:
    """The tests nextest reports green: `PASS`, or `LEAK` (passed, left a handle open)."""
    return {n for n, s in nextest_outcomes(out).items() if s in ("PASS", "LEAK")}


# nextest's real line shapes, each with the verdict the gate must give it.
_NEXTEST_SAMPLE = (
    "        SLOW [> 60.000s] (1/4) rivet-cli::live_suite m::slow_then_pass\n"
    "        PASS [  61.000s] (1/4) rivet-cli::live_suite m::slow_then_pass\n"
    "        LEAK [   0.400s] (2/4) rivet-cli::live_suite m::leaky_pass\n"
    " FAIL + LEAK [   0.476s] (3/4) rivet-cli::live_suite m::leaky_fail\n"
    "        FAIL [   0.200s] (4/4) rivet-cli::live_suite m::plain_fail\n"
    "        FAIL [   0.200s] (   5/1038) rivet-cli::live_suite m::padded_fail\n"
)


# A failed test's stderr as nextest prints it (once when it fails, once in the final list): a plain panic and the rig oracle's.
_NEXTEST_PANIC_SAMPLE = (
    "    thread 'm::plain_fail' (9992744) panicked at tests/live/m.rs:190:5:\n"
    "    P-00: delivered 6 of 10\n"
    "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n"
    "    thread 'm::oracle_fail' panicked at tests/common/verify.rs:1172:13:\n"
    "    rig oracle: export 't_62083_0' disagrees with its source / rivet's own ledger (rig_oracle.py grade):\n"
    "      - COUNT(*): source 10, delivered 6\n"
    "      - COUNT(`id`) (non-null): source 10, delivered 6\n"
    "    thread 'm::plain_fail' (9992744) panicked at tests/live/m.rs:190:5:\n"
    "    a later print of the same test\n"
)


def self_skip_error() -> str | None:
    """Why a self-skip marker would not be read, or None when it is."""
    import tempfile
    d = Path(tempfile.mkdtemp())
    # Two records interleaved the way parallel tests wrote them (line, line, newline, newline).
    (d / "s").write_text("RIVET-SKIP live_x::needs_bq — BIGQUERY_TEST_PROJECT unsetRIVET-SKIP live_x::b — no state\n\n")
    got = self_skipped(d / "s")
    want = {"live_x::needs_bq": "BIGQUERY_TEST_PROJECT unset", "live_x::b": "no state"}
    return None if got == want else f"read {got}"


def nextest_grading_error() -> str | None:
    """Why the nextest parser would misgrade a real line shape, or None when it grades all correctly."""
    if self_skip_error():
        return f"self-skips are not read: {self_skip_error()}"
    if nextest_started("    Starting 1038 tests across 1 binary (11 tests skipped)") != 1038:
        return "the `Starting N tests` count is not read"
    seen = set(nextest_outcomes(_NEXTEST_SAMPLE))
    every = {"m::slow_then_pass", "m::leaky_pass", "m::leaky_fail", "m::plain_fail", "m::padded_fail"}
    if seen != every:
        return f"read {sorted(seen)}, dropped {sorted(every - seen)} (a dropped FAIL line is a silent pass)"
    passed = nextest_passed(_NEXTEST_SAMPLE)
    want = {"m::slow_then_pass", "m::leaky_pass"}
    if passed != want:
        return f"graded green {sorted(passed)}, want {sorted(want)} (a FAIL + LEAK must stay red)"
    panics = nextest_panics(_NEXTEST_PANIC_SAMPLE)
    want_panics = {"m::plain_fail": "P-00: delivered 6 of 10", "m::oracle_fail": "rig oracle: COUNT(*): source 10, delivered 6"}
    if panics != want_panics:
        return f"read the panics {panics}, want {want_panics} (a known red is matched by this line)"
    return None


def verify_nextest_grading(led: "Ledger") -> None:
    """Refuse a gate whose parser would read a failed, leaky test as green."""
    why = nextest_grading_error()
    if why:
        led.failed("-", "harness", "nextest-grading", "-", f"nextest parser misgrades: {why}")
    else:
        led.passed("-", "harness", "nextest-grading", "-",
                   "nextest parser: FAIL + LEAK is red, LEAK green, SLOW not final", "ok")


def nextest_filter(tests: Sequence[str]) -> str:
    """A nextest `-E` expression selecting exactly these test fns, by their whole last path segment."""
    return " or ".join(f"test(/(^|::){t}$/)" for t in tests)


def test_passed(name: str, passed: set[str]) -> bool:
    """Whether the test fn `name` is among `passed` — a whole path segment, never a suffix of another name."""
    return any(q == name or q.endswith("::" + name) for q in passed)
