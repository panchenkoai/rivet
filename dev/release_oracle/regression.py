"""Release-oracle regression stage: the new binary vs the PREVIOUS RELEASE.

Port of `dev/release-oracle/lib/regression.sh`.

Every other stage of this gate compares rivet to a checked-in golden or to
itself. Neither notices a regression against the version users are ACTUALLY
running, and two such regressions are invisible to every correctness check yet
release-blocking:

* **format** — the new release must READ what the previous release WROTE
  (manifest + parts). A format bump that cannot open old artifacts breaks every
  existing user's data on upgrade, and does it quietly: worse than a crash.
* **perf / RSS** — a 3× slowdown or a memory blow-up ships GREEN through every
  count and value check there is.

The perf leg benchmarks against the DOWNLOADED previous-release binary
(`RIVET_PREV_RELEASE_BIN`: a GitHub release asset, or the Homebrew bottle) and
deliberately NOT against a locally rebuilt parent. Two reasons, both load-
bearing: the release profile is `lto = "fat"` with `codegen-units = 1`, so a
rebuild costs MINUTES per side; and what it produces is a local approximation,
not the artifact users run. Comparing to the published binary is both cheaper
and more honest. (`cargo install rivet-cli@<ver>` does not count either — it
rebuilds from source, which is the exact cost this avoids.)

Inputs. `RIVET_PREV_RELEASE_BIN` is the prev binary,
`RIVET_REGRESSION_SOURCE_URL` a Postgres this stage may seed a fixture into.
Wall tolerance is `RIVET_REGRESSION_WALL_TOL` (default 1.5×): a go/no-go catches
gross regressions, and fine-grained perf work belongs to the benchmark suite.

A MISSING PREVIOUS-RELEASE BINARY IS A FAILURE, NOT A SKIP. This is the one
place the general "a down service is SKIP" rule of the gate does NOT apply, and
0.24.4 is why: a governor regression (+1h48m of makespan, 52 exports shedding
workers in a field run) walked through a full release gate because the only
stage that compares rivet to the version users are running reported a
non-failure — it never ran, so it never disagreed with anything. "We could not
check" and "we checked and it was fine" are different facts, and a gate that
turns the first into a green table is worse than no gate: it manufactures
confidence out of an absence. A check that GRADES NOTHING MUST FAIL.

The escape is deliberate and named, never a default: `--without-prev-release-
comparison` (or `RIVET_ORACLE_WITHOUT_PREV_RELEASE=1`) turns these stages back
into SKIPs for a local partial run, and says out loud — in every row it records
and in the driver's own summary — that the run cannot support a tag. Everything
ELSE in this file keeps the ordinary SKIP contract: an absent
`RIVET_REGRESSION_SOURCE_URL` or scale URL is a down service, not a missing
baseline.

CROSS-VERSION STATE IS THE TRAP. The state DB (`.rivet_state.db`) lives next to
the config, so every binary here gets its OWN env directory. The new binary
UPGRADES the state schema on open (v18 → v19); the old binary then cannot open
what its successor migrated ("migration incomplete"). Sharing one state dir
between two versions fails the run for a reason that has nothing to do with the
regression being measured.
"""

from __future__ import annotations

import ast
import os
import re
import shutil
import signal
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from . import scenarios
from ..pytools.ab_regression import (
    CASE_TIMEOUT as _AB_CASE_TIMEOUT,
    RIVET_RUNS_TOTAL as _AB_CHILD_RUNS,
    TEARDOWN_WORST_CASE as _AB_CLEANUP_WORST_CASE,
)
from ..pytools.field_replay import (
    FLIP as _FR_FLIP,
    LEG_PLAN as _FR_LEG_PLAN,
    LEG_TIMEOUT as _FR_LEG_TIMEOUT,
    RESTORE_WORST_CASE as _FR_CLEANUP_WORST_CASE,
    STAND_FLIPPED_MARKER as _FR_FLIPPED,
    STAND_MUTATED_MARKER as _FR_MUTATED,
    STAND_RESTORED_MARKER as _FR_RESTORED,
    StandLock as _StandLock,
)
from ..pytools.shell import Fail as _ChildFail
from .core import (
    Ledger,
    Proc,
    ROOT,
    Status,
    container_for_port,
    docker_exec,
    port_of,
    rivet_bin,
    run,
)

# ── ledger identity ────────────────────────────────────────────────────────────
# The bash `add` calls in this file were missing the STORE column
# (`add release regression - SKIP "…"` is five fields where the ledger takes
# six), so every row it recorded was shifted: the status landed in STORE and the
# detail landed in STATUS, garbling the final table. The exit code survived only
# because bash tracked RED separately from the rows. Columns are named here, and
# `Ledger` derives the verdict from the rows, so the two cannot disagree.
_R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE = "release", "regression", "-", "-"
_S_VERSION, _S_SCENARIO, _S_STORE = "scale", "memory", "-"
# The two prev-release harness stages. `_AB_VERSION` / `_FR_VERSION` land in the
# VER column, so the final table groups them with the regression rows above
# rather than scattering them among the engines.
_AB_VERSION, _AB_STORE = "ab-diff", "-"
_FR_VERSION, _FR_STORE = "field", "-"

_MIB = 1048576


# ── running a CHILD HARNESS that owns a shared stand ──────────────────────────
#
# Two stages here shell out to a harness that mutates something outside its own
# process and cleans up on the way out: `field_replay` flips SERVER-WIDE
# tmp-table globals on `rivet-mysql-1` and restores them, `ab_regression` seeds a
# fixture table and drops it in the `finally` of the `fixture()` context manager
# that WRAPS the seed (the seed used to sit one line above that `try`, so an
# interrupt during it leaked a 50 000-row table). Both cleanups were reachable by
# every signal EXCEPT the one this wrapper actually sent.
#
# `core.run` is `subprocess.run(timeout=…)`, and CPython implements that timeout
# with `Popen.kill()` — SIGKILL. SIGKILL runs no `__exit__`, no signal handler
# and no `atexit`, so a wrapper timeout left the shared MySQL server sitting at
# `internal_tmp_mem_storage_engine=MEMORY / tmp_table_size=16384` for every later
# test and every later run (exactly the poisoned stand found on 2026-08-14), and
# left one 50 000-row `ab_src_<pid>` table behind per timed-out run.
#
# Two things are wrong with that and both are fixed here:
#
#   1. THE BUDGETS WERE CONFLATED. The wrapper read the SAME env var as the
#      harness's own PER-LEG / PER-CASE budget, so the wrapper's whole-harness
#      timeout was always the smaller number (X vs 4X for field_replay, X vs
#      ~20X for ab_regression) and fired FIRST — the harness's own graceful
#      leg-timeout escape could never be reached under the oracle, no matter
#      what an operator set. Each wrapper now has its OWN override and derives
#      its default from the child's budget times the child's own unit count.
#
#   2. THE SIGNAL WAS UNCATCHABLE. `_run_interruptible` sends SIGINT first and
#      waits out a grace period before escalating. SIGINT — not SIGTERM — is the
#      terminating signal BOTH children honour: field_replay installs a handler
#      that restores the globals and re-raises, and ab_regression relies on
#      Python's default SIGINT→KeyboardInterrupt, which runs its `finally`.
#      (Python's DEFAULT SIGTERM disposition exits without unwinding, so SIGTERM
#      would still skip ab_regression's `DROP TABLE`.) SIGKILL remains the last
#      resort for a child that ignores SIGINT for the whole grace period, and
#      that case says so in the transcript instead of being silent.
#
#   3. THE GRACE PERIOD WAS A GUESS ABOUT SOMEONE ELSE'S CODE. Both BUDGETS above
#      are derived from the child's own imported constants; the grace — the
#      number the whole SIGINT fix depends on — was typed as a flat 300 s, while
#      field_replay's restore retries 3 × (SET + read-back, each capped at 600 s)
#      with a backoff: ~3602 s worst case. So a WEDGED container (precisely the
#      condition that makes the wrapper's timeout fire AND the restore slow) was
#      SIGKILLed mid-restore, leaving a partially-restored stand under a
#      transcript asserting "the child's cleanup did NOT run". Each call site now
#      passes the child's OWN cleanup worst case (`RESTORE_WORST_CASE` /
#      `TEARDOWN_WORST_CASE`), imported like the budgets.
_GRACE_ENV = "RIVET_ORACLE_CHILD_GRACE"
# How long to keep draining a SIGKILLed child's pipes. See `_run_interruptible`.
_KILL_DRAIN = 60.0


def _child_grace(cleanup_worst_case: float) -> float:
    """Seconds to wait after SIGINT before SIGKILL, for a child whose cleanup
    takes at most `cleanup_worst_case`.

    Read at CALL time and parsed explicitly. The one-liner it replaces —
    `float(os.environ.get(_GRACE_ENV) or 300)` — got two things wrong at once,
    both verified by running it:

    * `RIVET_ORACLE_CHILD_GRACE=0` produced 0.0, not the documented default,
      because `"0"` is a truthy STRING and `or` never fires. That is the same
      `"0"`-is-truthy grammar bug this very commit fixed in `__main__.env_flag`,
      and it lands on the natural spelling of "no grace" — i.e. it silently
      restores the pre-fix SIGKILL-immediately behaviour. Zero is now HONOURED
      (this is a duration, and "wait zero" is a coherent thing to ask for) but
      never quietly: it prints what it costs, so the transcript carries the
      reason a stand came back mutated.
    * a non-numeric value raised ValueError at IMPORT time, taking the whole gate
      down before it printed a line. A malformed knob now warns and falls back.
    """
    raw = os.environ.get(_GRACE_ENV, "").strip()
    if not raw:
        return cleanup_worst_case
    try:
        secs = float(raw)
    except ValueError:
        print(f"  ! {_GRACE_ENV}={raw!r} is not a number — using the derived "
              f"{cleanup_worst_case:.0f}s grace")
        return cleanup_worst_case
    if secs < 0:
        print(f"  ! {_GRACE_ENV}={raw!r} is negative — using the derived "
              f"{cleanup_worst_case:.0f}s grace")
        return cleanup_worst_case
    if secs == 0:
        print(f"  ! {_GRACE_ENV}=0 — a timed-out child harness will be SIGKILLed with NO "
              "chance to run its cleanup; the shared stand (MySQL tmp-table globals / the "
              "seeded fixture table) may be left mutated, and the stand row below is the "
              "only thing that will say so")
    elif secs < cleanup_worst_case:
        print(f"  ! {_GRACE_ENV}={secs:.0f}s is shorter than the child's own cleanup worst "
              f"case ({cleanup_worst_case:.0f}s) — a slow cleanup will be SIGKILLed part-way")
    return secs


def _wrapper_budget(own_env: str, child_budget: float, units: int, slack: float) -> float:
    """The WRAPPER's timeout for a child that enforces its own per-unit budget.

    Always strictly greater than the child's worst case, so the child's own
    timeout handling is what fires in a slow run and this one only ever catches
    a genuine HANG. `own_env` overrides it outright — one operator knob per
    wrapper, distinct from the child's.
    """
    explicit = os.environ.get(own_env)
    if explicit:
        return float(explicit)
    return child_budget * units + slack


def _run_interruptible(argv, *, timeout: float, env: dict[str, str], cwd: Path,
                       grace: float) -> Proc:
    """`core.run`, except the timeout is SIGINT + grace + SIGKILL rather than an
    immediate SIGKILL. Returns 124 when the child cleaned up and exited within
    the grace period, 137 when it had to be killed — a distinction the ledger
    row repeats, because only the second leaves the stand's state unknown.

    `grace` has no default: every caller states the CLEANUP it is waiting for
    (`_child_grace(<child>.WORST_CASE)`), so a new child harness cannot inherit
    a number that was sized for a different one's teardown.
    """
    full_env = {**os.environ, **env}
    with subprocess.Popen(
        list(argv), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=full_env, cwd=str(cwd),
    ) as proc:
        try:
            out, err = proc.communicate(timeout=timeout)
            return Proc(argv, proc.returncode, out, err)
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGINT)
            try:
                out, err = proc.communicate(timeout=grace)
                return Proc(argv, 124, out, err + (
                    f"\n[timeout after {timeout}s — SIGINT sent; the harness ran its cleanup "
                    f"and exited within {grace}s]"))
            except subprocess.TimeoutExpired:
                proc.kill()
                # BOUNDED. SIGKILL ends the CHILD, but the pipes stay open as long
                # as any GRANDCHILD holds the inherited fd — a `docker exec`, a
                # `rivet` leg — so an unbounded `communicate()` here can block
                # forever, with the whole gate hung and not one line of diagnostic
                # (the child's output is still inside this call). Draining is
                # best-effort; the transcript is worth waiting a minute for, and
                # not one second more.
                try:
                    out, err = proc.communicate(timeout=_KILL_DRAIN)
                    drained = ""
                except subprocess.TimeoutExpired:
                    out, err = "", ""
                    drained = (
                        f" Its output could NOT be drained within {_KILL_DRAIN:.0f}s after the "
                        "kill — something it spawned still holds the pipes open, so the "
                        "transcript below is empty and any grandchild is still running.")
                return Proc(argv, 137, out, err + (
                    f"\n[timeout after {timeout}s — SIGINT ignored for {grace}s, SIGKILLed; "
                    "the child's cleanup did NOT run and the stand may be left mutated]"
                    + drained))


# ── timing / RSS measurement ───────────────────────────────────────────────────
@dataclass(frozen=True)
class Timing:
    """One measured run.

    `wall` is the string as it is PRINTED (kept verbatim from the parse so the
    log reads identically to the bash), `secs` the value actually COMPARED, and
    `rss` peak resident bytes, and `ok` whether the timed command actually
    SUCCEEDED.

    `ok` exists because a failed run still yields a perfectly plausible
    measurement — a binary that refuses to start reports ~11 MB and 0.01s, which
    reads as "leaner and faster" rather than "never ran". Measured 2026-08-03:
    the prev binary failed on all four engines and the report said
    `prev[users=11MB events=11MB flat×1.00]`, which a reader would take as a 3x
    RSS regression in the current tree.
    """

    wall: str
    secs: float
    rss: int
    ok: bool = True


# `/usr/bin/time` speaks two dialects: BSD/macOS `-l` and GNU `-v`. Detect by
# probing `-l` on a trivial command — NEVER by the exit status of the timed
# command itself, which is what a naive `||` fallback does: a run that
# legitimately returns non-zero would then be re-run under the wrong flag and
# its timing lost. Probed once per process; the dialect cannot change under us.
_TIME_BIN = Path("/usr/bin/time")
_bsd: bool | None = None


def _is_bsd_time() -> bool:
    global _bsd
    if _bsd is None:
        _bsd = run([str(_TIME_BIN), "-l", "true"], timeout=60).ok
    return _bsd


def _parse_wall(text: str) -> tuple[str, float]:
    """Wall seconds from either dialect, as (display string, value).

    BSD prints `0.50 real`; GNU prints
    `Elapsed (wall clock) time (h:mm:ss or m:ss): 0:00.50`. Both patterns are
    tried regardless of the detected dialect, which keeps the parse tolerant.

    The GNU form is where the bash was WRONG: it grepped the field for
    `[0-9]+\\.[0-9]+`, which picks up only the seconds component, so a 1:23.45
    run measured as 23.45 s — a 90 s regression could read as a 30 s pass. Here
    the colon-separated field is summed properly. The display string stays byte-
    identical to the bash in the sub-minute case it did handle (that is every
    run of this fixture, which is sized to be sub-second) and shows the CORRECTED
    value in the case it did not, rather than printing a number the verdict
    disagrees with.
    """
    m = re.search(r"(\d+\.\d+) real", text)
    if m:
        return m.group(1), float(m.group(1))

    # The label itself contains colons (`(h:mm:ss or m:ss)`), so the field is
    # anchored at end-of-line rather than found by the first colon after it.
    m = re.search(
        r"wall clock[^\n]*?:\s*([0-9]+(?::[0-9]+)*(?:\.[0-9]+)?)\s*$", text, re.M
    )
    if m:
        field = m.group(1)
        total = 0.0
        for part in field.split(":"):
            total = total * 60 + float(part)
        # What the bash would have shown: the last `d+.d+` token in the field.
        toks = re.findall(r"[0-9]+\.[0-9]+", field)
        legacy = toks[-1] if toks else ""
        if legacy and abs(float(legacy) - total) < 1e-9:
            return legacy, total
        return f"{total:.2f}", total

    return "0", 0.0


def _parse_rss(text: str) -> int:
    """Peak resident bytes from either dialect (BSD reports bytes, GNU kbytes)."""
    m = re.search(r"(\d+) +maximum resident set size", text)
    if m:
        return int(m.group(1))
    m = re.search(r"Maximum resident set size[^0-9]*(\d+)", text)
    if m:
        return int(m.group(1)) * 1024
    return 0


def _regr_time(binary: Path | str, *args: str) -> Timing:
    """Run `binary args…` under `/usr/bin/time`, returning wall + peak RSS.

    No timeout, as in the bash: an export of the fixture legitimately takes as
    long as it takes, and a killed run would read as a real failure.

    When `/usr/bin/time` is absent entirely (some minimal Linux images ship only
    the shell builtin) both dialects fail and this returns zeros, which the
    callers' `> 0` guards turn into a FAIL rather than a SKIP. That is the bash
    behaviour, kept deliberately — but it is a misclassification worth knowing
    about if this ever fires on a container with no coreutils `time`.
    """
    flag = "-l" if _is_bsd_time() else "-v"
    p = run([str(_TIME_BIN), flag, str(binary), *args], timeout=None,
            env=_ISOLATED_STATE)
    # The report goes to stderr, mixed with whatever the timed command wrote
    # there — the same stream the bash captured into its temp file.
    wall, secs = _parse_wall(p.stderr)
    return Timing(wall, secs, _parse_rss(p.stderr), p.returncode == 0)


def _tolerance(raw: str) -> float:
    """A tolerance env var as a number, coercing junk to 0 the way awk did —
    which makes `cur <= prev * 0` false, i.e. a nonsense tolerance FAILS loudly
    instead of being silently treated as "no limit"."""
    try:
        return float(raw)
    except ValueError:
        return 0.0


# ── source access ──────────────────────────────────────────────────────────────
# This cell's whole design is one state DB PER BINARY (see `_regr_cfg`), so that
# the current tree's schema upgrade never touches the previous release's state.
# `--state-url` defeats that: `__main__` sets `RIVET_STATE_URL` process-wide and
# every child inherits it, so both binaries meet ONE Postgres state DB — and the
# published binary correctly refuses a schema the current tree just migrated
# ("state(pg): migration incomplete — expected schema v19 but reached v20").
#
# Measured 2026-08-03: the whole cell failed on the Postgres pass for exactly
# this reason (`prev-export-failed`, and every later fragment was a consequence
# of a baseline that never ran), while the same prev binary with its own SQLite
# state exported 100000 rows into 4 parts, exit 0. Left alone, this cell can
# NEVER pass on a Postgres pass once the tree adds a migration — which is
# precisely when a cross-version check earns its keep.
#
# An empty value means "SQLite beside the config" (verified against the PUBLISHED
# binary, not just the current tree), and `run`'s env MERGES over os.environ, so
# clearing requires the empty string rather than a missing key.
_ISOLATED_STATE = {"RIVET_STATE_URL": ""}


def _regr_psql(url: str, sql: str) -> Proc:
    """Run `sql` against the regression source, via the container publishing its
    port (the URL points at a docker-published 127.0.0.1 port, so `psql` inside
    the container is the one client guaranteed to exist).

    Returns a Proc with returncode 1 when no container publishes that port,
    rather than handing an empty container name to `docker exec` (which answers
    "invalid container name or ID: value is empty").
    """
    port = port_of(url)
    name = container_for_port(port) if port is not None else None
    if name is None:
        return Proc(["psql", url], 1, "", f"no running container publishes port {port}")
    return docker_exec(
        name, "psql", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-q",
        stdin=sql, timeout=None,
    )


# ── config writers ─────────────────────────────────────────────────────────────
def _regr_cfg(src: str, envdir: Path, dest: Path | None = None) -> Path:
    """A keyset+zstd export of the fixture into an ISOLATED env dir.

    State and output both live under `envdir`, which is the whole point: the
    state DB sits beside the config, so one dir per binary keeps the new
    version's schema upgrade away from the old version's DB. `dest` overrides
    the destination so one binary can be pointed at ANOTHER's output.
    """
    (envdir / "out").mkdir(parents=True, exist_ok=True)
    out = envdir / "out" if dest is None else dest
    cfg = envdir / "c.yaml"
    cfg.write_text(
        f'source: {{ type: postgres, url: "{src}" }}\n'
        "exports:\n"
        "  - name: regr_probe\n"
        "    table: regr_probe\n"
        "    mode: chunked\n"
        "    chunk_by_key: id\n"
        "    chunk_size: 50000\n"
        "    format: parquet\n"
        "    compression: zstd\n"
        f'    destination: {{ type: local, path: "{out}/" }}\n'
    )
    return cfg


def _scale_cfg(engine: str, url: str, table: str, envdir: Path) -> Path:
    """A batch export of an EXISTING table — keyset for the SQL engines, full
    scan for Mongo — into its own env dir (per-binary isolation, as above)."""
    tls = "\n  tls: { accept_invalid_certs: true }" if engine == "mssql" else ""
    if engine == "mongo":
        mode = "    mode: full"
    else:
        chunk = os.environ.get("RIVET_SCALE_CHUNK") or "100000"
        mode = f"    mode: chunked\n    chunk_by_key: id\n    chunk_size: {chunk}"
    (envdir / "out").mkdir(parents=True, exist_ok=True)
    cfg = envdir / "c.yaml"
    cfg.write_text(
        "source:\n"
        f"  type: {engine}\n"
        f'  url: "{url}"{tls}\n'
        "exports:\n"
        f"  - name: {table}\n"
        f"    table: {table}\n"
        f"{mode}\n"
        "    format: parquet\n"
        "    compression: zstd\n"
        f'    destination: {{ type: local, path: "{envdir}/out/" }}\n'
    )
    return cfg


def prev_binary() -> Path | None:
    """The DOWNLOADED previous-release binary, or None.

    `is_file()` rather than a bare executable test: `[ -x ]` is also true for a
    directory, and a path that happens to name one would get as far as trying to
    exec it.

    PUBLIC because the driver's banner and its closing `NOT RELEASE-GRADED` line
    must ask the SAME question the stages ask. They used to branch on the ESCAPE
    FLAG alone, which is not the same fact: `_require_prev_binary` returns the
    baseline BEFORE consulting the flag, so a run with a baseline in the shell
    and the flag set (exactly `RIVET_PREV_RELEASE_BIN=… make release-oracle`,
    since that target passes the flag unconditionally) RAN all three comparison
    stages and recorded real PASS/FAIL rows while the banner said they would
    SKIP and the last line said nothing had compared anything.
    """
    raw = os.environ.get("RIVET_PREV_RELEASE_BIN", "")
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_file() and os.access(p, os.X_OK) else None


# The named escape. Read from the ENVIRONMENT rather than threaded down as a
# parameter, because that is how every other gate-wide knob reaches this layer
# (`RIVET_STATE_URL`, `BLESS_*`): `__main__` exports it once after parsing argv,
# and `core.run` merges os.environ into every child. It also means a CI job that
# can set env but not argv has the same one switch, spelled the same way.
_ESCAPE_FLAG = "--without-prev-release-comparison"
_ESCAPE_ENV = "RIVET_ORACLE_WITHOUT_PREV_RELEASE"


def without_prev_release_comparison() -> bool:
    """True when the operator has DELIBERATELY given up every comparison against
    the previously released binary. False (the default) means a missing baseline
    FAILS the run — see the module docstring."""
    return os.environ.get(_ESCAPE_ENV, "").strip().lower() not in ("", "0", "false", "no", "off")


def set_without_prev_release_comparison(on: bool) -> None:
    """Publish the escape into the environment (the driver calls this once, after
    parsing argv). One writer, one reader, one spelling — the flag and the env
    var are the same switch rather than two facts that can drift apart."""
    if on:
        os.environ[_ESCAPE_ENV] = "1"
    else:
        os.environ.pop(_ESCAPE_ENV, None)


def prev_release_banner(prev: Path | None, escape: bool) -> tuple[list[str], str | None]:
    """What the driver PRINTS about the previous-release comparison: the start-up
    banner lines, and the closing `NOT RELEASE-GRADED` line (or None).

    Pure, and keyed on the BASELINE rather than on the flag, because the flag is
    not what decides: `_require_prev_binary` returns the baseline before it ever
    consults the escape, so a present baseline is COMPARED whatever the flag
    says. Branching on the flag alone made `RIVET_PREV_RELEASE_BIN=… make
    release-oracle` (the bare target passes the escape unconditionally) announce
    a skip, run all three stages for real, and then close with "nothing above
    compared this binary to the release users are running" over a table full of
    genuine PASS/FAIL rows. The banner and the footer must describe what HAPPENED.
    """
    if prev is not None:
        lines = [f"  previous-release baseline: {prev}",
                 "    → the regression, differential and field-replay stages RUN and GRADE"]
        if escape:
            # Not a contradiction to smooth over: the escape only covers an
            # ABSENT baseline, and saying so is the difference between a reader
            # trusting these rows and discarding them.
            lines.append(
                f"    → {_ESCAPE_FLAG} was passed, but a baseline IS present: the escape "
                "only excuses an ABSENT one, so nothing is given up and this run DOES "
                "grade against the previous release")
        return lines, None
    if escape:
        return ([f"  previous-release comparison: GIVEN UP ({_ESCAPE_FLAG}), and no baseline "
                 "is present",
                 "    → the regression, differential and field-replay stages SKIP instead of "
                 "FAIL; this run CANNOT support a tag"],
                f"  NOT RELEASE-GRADED: no previous-release baseline was used and "
                f"{_ESCAPE_FLAG} turned those stages into SKIPs — nothing above compared "
                "this binary to the release users are running.")
    return (["  previous-release baseline: <ABSENT — the comparison stages will FAIL>"],
            "  NOT RELEASE-GRADED: no previous-release baseline was used — nothing above "
            "compared this binary to the release users are running (those stages FAILED "
            f"for that reason; {_ESCAPE_FLAG} is the named way to give them up).")


def _require_prev_binary(
    led: Ledger, engine: str, version: str, scenario: str, store: str, what: str
) -> Path | None:
    """The previous-release binary, or None with the row already recorded.

    Absent baseline ⇒ **FAIL** by default, **SKIP** only under the named escape.
    One helper for all three stages so they cannot drift into disagreeing about
    what an absent baseline means — the drift is exactly how the 0.24.4 gate
    ended up with one leg that could not fail.
    """
    prev = prev_binary()
    if prev is not None:
        return prev
    raw = os.environ.get("RIVET_PREV_RELEASE_BIN", "")
    why = (
        f"RIVET_PREV_RELEASE_BIN={raw!r} is not an executable file"
        if raw
        else "RIVET_PREV_RELEASE_BIN is unset"
    )
    if without_prev_release_comparison():
        led.skipped(
            engine, version, scenario, store,
            f"{what}: {why} — GIVEN UP on purpose by {_ESCAPE_FLAG}. "
            "This run does NOT grade the release against the binary users are "
            "running and cannot support a tag.",
            "no prev binary (escape: NOT release-graded)",
        )
        return None
    led.failed(
        engine, version, scenario, store,
        f"{what}: {why} — a release run must be GRADED against the binary users "
        "are actually running, and a check that never ran is not a check. "
        "(0.24.4 shipped a +1h48m governor regression through a full green gate "
        "for exactly this reason: its only comparison leg SKIPped.) Get the "
        "baseline with `make release-oracle-prev-bin` / `make "
        f"release-oracle-full`, or state the loss out loud with {_ESCAPE_FLAG} "
        "for a local partial run.",
        "no prev binary (release runs REQUIRE one)",
    )
    return None


# ── stage 1: format + perf vs the previous release ─────────────────────────────
def verify_release_regression(led: Ledger) -> None:
    prev = _require_prev_binary(
        led, _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE, "release regression"
    )
    if prev is None:
        return
    src = os.environ.get("RIVET_REGRESSION_SOURCE_URL", "")
    if not src:
        led.skipped(
            _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
            "release regression: no RIVET_REGRESSION_SOURCE_URL", "no source",
        )
        return

    tol = os.environ.get("RIVET_REGRESSION_WALL_TOL") or "1.5"
    tol_n = _tolerance(tol)
    prev_ver = run([str(prev), "--version"]).stdout.splitlines()
    led.phase(
        f"Release regression vs prev ({prev_ver[0] if prev_ver else ''}) — "
        "cross-version read + perf/RSS"
    )
    # Not auto-deleted: the exports under here are what a reader wants to open
    # when the format leg fails, and the bash trap removed them before anyone
    # could look.
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-regr-"))

    # A seeded 100K-row fixture: deterministic, measurable, and sub-second, so
    # the perf comparison is not at the mercy of whatever happens to be on the
    # stand.
    _regr_psql(src, """DROP TABLE IF EXISTS regr_probe;
CREATE TABLE regr_probe (id int PRIMARY KEY, a text, b numeric(18,4), c timestamptz);
INSERT INTO regr_probe SELECT g, md5(g::text), (g%1000)+0.25, '2025-01-01'::timestamptz + (g||' seconds')::interval FROM generate_series(1,100000) g;
""")

    # Fragments carry their own trailing space and are joined verbatim, so the
    # rendered message and the recorded detail match the bash byte for byte.
    fails: list[str] = []

    # ── format: prev WRITES → cur READS its manifest + parts (the upgrade path) ──
    pe = work / "prev_fmt"
    _regr_cfg(src, pe)
    if not run([str(prev), "run", "-c", str(pe / "c.yaml")], timeout=None, env=_ISOLATED_STATE).ok:
        fails.append("prev-export-failed ")
    # cur validates prev's OUTPUT from cur's OWN env, so cur's state-schema
    # upgrade never touches prev's state DB.
    ce = work / "cur_fmt"
    _regr_cfg(src, ce, pe / "out")
    validated = run([str(rivet_bin()), "validate", "-c", str(ce / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    if "passed" not in validated.out.lower():
        fails.append("cur-cannot-read-prev-output(format-break) ")

    # An INDEPENDENT reader over the same parts: DuckDB, not rivet, must agree
    # with the source row count — otherwise "cur can read prev's output" rests
    # entirely on rivet's own verdict about itself.
    # DECLARED parts, not a glob: the claim being graded is "the current binary
    # can READ what the previous one DELIVERED", and delivery is what the prior
    # run's manifest names. A glob would also read parts it abandoned.
    _src = scenarios._declared_read(pe / "out", ".parquet")
    duck = run([
        "duckdb", "-noheader", "-list", "-c",
        f"SELECT count(*) FROM read_parquet({_src})",
    ]) if _src else None
    dcnt = duck.stdout.strip() if duck else ""
    m = re.search(r"\d+", _regr_psql(src, "SELECT count(*) FROM regr_probe;").stdout)
    scnt = m.group(0) if m else ""
    if not dcnt or dcnt != scnt:
        fails.append(f"prev-parts-rowcount[{dcnt}!={scnt}] ")

    # ── perf: cur vs the downloaded prev release, each in its own env ──
    # One warm pass per binary (page the binary in, prime the page cache), then
    # a timed pass into a FRESH output dir, so the measurement is one clean run
    # rather than a run plus whatever the warm-up left behind.
    pp, pc = work / "perf_prev", work / "perf_cur"
    _regr_cfg(src, pp)
    _regr_cfg(src, pc)
    run([str(prev), "run", "-c", str(pp / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    run([str(rivet_bin()), "run", "-c", str(pc / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    for d in (pp / "out", pc / "out"):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
    pt = _regr_time(prev, "run", "-c", str(pp / "c.yaml"))
    ct = _regr_time(rivet_bin(), "run", "-c", str(pc / "c.yaml"))
    if not (ct.secs > 0 and pt.secs > 0 and ct.secs <= pt.secs * tol_n):
        fails.append(f"perf-regression(cur {ct.wall}s > prev {pt.wall}s ×{tol}) ")

    _regr_psql(src, "DROP TABLE IF EXISTS regr_probe;")

    rssnote = ""
    if ct.rss > 0 and pt.rss > 0:
        rssnote = f" RSS cur {ct.rss // _MIB}MB vs prev {pt.rss // _MIB}MB"
    if not fails:
        led.passed(
            _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
            "release regression: cur reads prev's output (format-compat), "
            f"perf cur {ct.wall}s <= prev {pt.wall}s ×{tol}{rssnote}",
            f"wall {ct.wall}/{pt.wall}s",
        )
    else:
        joined = "".join(fails)
        led.failed(_R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
                   f"release regression: {joined}", joined)


# ── stage 2: the FLAT-RSS guarantee at scale ───────────────────────────────────
def verify_scale_memory(led: Ledger) -> None:
    """rivet STREAMS: peak RSS is O(chunk), NOT O(rows).

    That is the headline value proposition — the 454M-row, no-OOM field win over
    tools that buffer — and nothing else in this gate can see it break. The data
    scenarios run ~150K rows, so a regression that reintroduces buffering (RSS
    scaling with the table) ships GREEN through every count and value check.

    The check runs PER ENGINE over TWO tables ALREADY ON THE STAND (no seeding —
    a small one and a large one, e.g. `users` vs `events`): peak RSS on the large
    table must stay flat against the small one,
    `RSS(large) <= RSS(small) × RIVET_SCALE_RSS_TOL` (default 3×). The tolerance
    is deliberately generous because the two tables differ in ROW WIDTH:
    streaming keeps the ratio near the width ratio, while buffering blows it up
    by the ROW-COUNT ratio — orders of magnitude apart, so a loose bound still
    separates them cleanly. Measured for the current binary (that is the
    assertion) and, when available, the downloaded prev release (context only).

    Per engine, set `RIVET_SCALE_<ENGINE>_URL` plus optionally
    `RIVET_SCALE_<ENGINE>_SMALL` / `_LARGE` (default `users` / `events`). An
    engine with no URL SKIPs — never a silent pass.
    """
    prev = prev_binary()
    tol = os.environ.get("RIVET_SCALE_RSS_TOL") or "3"
    tol_n = _tolerance(tol)
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-scale-"))

    for engine in ("postgres", "mysql", "mssql", "mongo"):
        up = engine.upper()
        uvar = f"RIVET_SCALE_{up}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                        f"scale[{engine}]: no {uvar}", "no url")
            continue
        small = os.environ.get(f"RIVET_SCALE_{up}_SMALL") or "users"
        large = os.environ.get(f"RIVET_SCALE_{up}_LARGE") or "events"
        led.phase(
            f"Scale memory [{engine}] (FLAT-RSS: existing table '{large}' vs "
            f"'{small}' — streaming, not buffering, tol {tol}×)"
        )

        fails: list[str] = []
        report = ""
        for label in ("cur", "prev"):
            if label == "cur":
                binary: Path = rivet_bin()
            elif prev is None:
                continue  # no downloaded prev release: the cur assertion stands alone
            else:
                binary = prev
            sdir = work / f"{engine}_{label}_s"
            ldir = work / f"{engine}_{label}_l"
            _scale_cfg(engine, url, small, sdir)
            _scale_cfg(engine, url, large, ldir)
            st = _regr_time(binary, "run", "-c", str(sdir / "c.yaml"))
            lt = _regr_time(binary, "run", "-c", str(ldir / "c.yaml"))
            if not (st.ok and lt.ok):
                # Never print a number for a run that did not happen: a failed
                # binary's ~11 MB reads as a lean baseline and invites exactly
                # the wrong conclusion about the other column.
                report += f" {label}[RUN FAILED — no measurement]"
            else:
                ratio = f"{lt.rss / st.rss:.2f}" if st.rss > 0 else "NA"
                report += (
                    f" {label}[{small}={st.rss // _MIB}MB "
                    f"{large}={lt.rss // _MIB}MB flat×{ratio}]"
                )
            # Only the CURRENT binary is asserted on; prev is reported for
            # context, since a prev that also buffers is not a reason to ship.
            if label == "cur" and not (st.ok and lt.ok):
                fails.append("cur-run-failed(no RSS measurement to assert on) ")
            elif label == "cur" and not (st.rss > 0 and lt.rss > 0 and lt.rss <= st.rss * tol_n):
                fails.append(
                    f"rss-NOT-flat(cur {large}={lt.rss // _MIB}MB > "
                    f"{small}={st.rss // _MIB}MB ×{tol} — buffering, not streaming) "
                )

        if not fails:
            led.passed(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                       f"scale[{engine}]: flat-RSS holds —{report}", report)
        else:
            joined = "".join(fails)
            led.failed(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                       f"scale[{engine}]: {joined} —{report}", joined)


# ── stage 3: the observable-surface differential vs the previous release ───────
#
# `dev/pytools/ab_regression.py` runs BOTH binaries over identical fixtures and
# diffs everything a user can observe: exit code, rows read back by DuckDB (not
# by rivet), file count, and the manifest's own accounting down to per-part
# content fingerprints and per-column checksums. Stage 1 above asks "can the new
# binary READ what the old one wrote, and is it no slower?"; this asks the
# complementary question no self-comparison can — "does it still DO the same
# thing?" — over the runners a release actually ships on.
#
# The harness is the oracle; this wrapper's job is only to run it in the gate's
# environment and to turn its output into ledger rows. Two things it must not
# get wrong:
#
#   * THE COUNT. A harness that compared zero scenarios also reports "no
#     difference". So the number of scenarios GRADED is asserted, not just the
#     verdict, and it is recorded in the row a reader sees.
#   * THE STATE DIR. Two binary VERSIONS run here, so `RIVET_STATE_URL` must be
#     cleared exactly as stage 1 clears it (`_ISOLATED_STATE`): under a shared
#     Postgres state DB the new binary migrates the schema and the published one
#     then refuses to open it, which reads as a differential failure and is not.
_AB_MODULE = "dev.pytools.ab_regression"

# A RATCHET, not a description: the harness compared 8 scenarios (6 config
# shapes + crash_resume + surfaces) when this stage was written. Fewer means the
# differential shrank — deliberately or by a harness that died half-way — and
# either way the gate must say so instead of reporting agreement over a
# remainder. Raise it when scenarios are added; lowering it is an explicit act.
_AB_MIN_SCENARIOS = 8

# The harness runs `RIVET_RUNS_TOTAL` rivet invocations, each capped at its OWN
# `CASE_TIMEOUT` (`RIVET_AB_TIMEOUT`, default 900 s). BOTH numbers are IMPORTED
# from the harness rather than re-typed — the field path already did this with
# `LEG_PLAN` / `LEG_TIMEOUT`, and this path (the one whose scenario list actually
# grows) re-typed a 20 and a 900 instead: a 7th scenario would push the child's
# worst case to 19 800 s against a wrapper budget of 18 900 s and the wrapper, not
# the harness, would fire first again. The harness's own self-test COUNTS its
# invocations against `RIVET_RUNS_TOTAL`, so the imported number is graded too.
# Slack covers seed + preflight + the DuckDB readbacks. Override outright with
# `RIVET_ORACLE_AB_TIMEOUT`.
_AB_SLACK = 900.0


def _ab_budget() -> float:
    return _wrapper_budget(
        "RIVET_ORACLE_AB_TIMEOUT", _AB_CASE_TIMEOUT, _AB_CHILD_RUNS, _AB_SLACK
    )


# `{name:20} {field:16} {old:>22} {new:>22}  {verdict}` — only the name, the
# field and the verdict are load-bearing here; the two value columns can contain
# spaces (a truncated dict), so they are matched loosely.
_AB_ROW = re.compile(r"^(\S+)\s+(\S+)\s+.*\s(same|DIFFERS)\s*$")
# The harness's own two self-audit blocks, which the table does NOT contain:
#   `  ✗ <cell>: …`  a cell that GRADED NOTHING (both sides empty, the injected
#                    crash never fired, the error path exited 0) — fatal there,
#                    and fatal here: it is the vacuous green in miniature.
#   `  ! <cell>: …`  a cell that graded LESS than it claims (no manifest to
#                    compare, DuckDB could not read either side) — the harness
#                    calls this loud-but-not-fatal, and this wrapper does NOT
#                    overrule it; it carries the note onto the row so a reader
#                    sees what was not compared.
_AB_DEAD = re.compile(r"^\s*✗\s+(.*)$")
_AB_PARTIAL = re.compile(r"^\s*!\s+(.*)$")


@dataclass(frozen=True)
class AbReport:
    """What the differential harness said, per scenario."""

    order: list[str]                    # scenarios in the order graded
    diffs: dict[str, list[str]]         # scenario -> fields that DIFFER
    dead: dict[str, list[str]]          # scenario -> "this graded nothing" notes
    partial: dict[str, list[str]]       # scenario -> "this graded less" notes
    # Notes that name no scenario the table listed, kept split by severity: an
    # unattributable DEAD note is still a cell that graded nothing (fatal), while
    # an unattributable PARTIAL note is usually housekeeping — the harness's own
    # "could not drop the fixture" warning has exactly this shape — and must not
    # turn a release red.
    loose_dead: list[str]
    loose_partial: list[str]


def _ab_parse(text: str) -> AbReport:
    """Parse the harness's transcript.

    FOUR sources, deliberately, because each carries something the others do
    not: the per-field TABLE gives the inventory (which scenarios ran at all);
    the trailing `N DIFFERENCE(S)` tuple block is the harness's authoritative
    regression list and carries findings the table never prints (`expected_rows`
    — the guard that fires when BOTH binaries are broken the same way and every
    table row therefore reads "same"); and the two self-audit blocks say which
    cells graded nothing / graded less than they claim.
    """
    order: list[str] = []
    diffs: dict[str, list[str]] = {}
    dead: dict[str, list[str]] = {}
    partial: dict[str, list[str]] = {}
    loose_dead: list[str] = []
    loose_partial: list[str] = []

    def note(name: str, field: str | None = None) -> None:
        if name not in order:
            order.append(name)
        if field is not None and field not in diffs.setdefault(name, []):
            diffs[name].append(field)

    def attribute(bucket: dict[str, list[str]], loose: list[str], text_: str) -> None:
        # "<cell>: <why>" — attributed only to a cell the TABLE actually named,
        # so an unrelated line (the fixture-teardown warning, which has the same
        # shape) cannot invent a scenario. Anything else is carried whole.
        head, _, rest = text_.partition(":")
        if rest and head.strip() in order:
            bucket.setdefault(head.strip(), []).append(rest.strip())
        else:
            loose.append(text_.strip())

    for line in text.splitlines():
        m = _AB_ROW.match(line.rstrip())
        if m:
            name, field, verdict = m.groups()
            note(name, field if verdict == "DIFFERS" else None)
            continue
        s = line.strip()
        if s.startswith("(") and s.endswith(")"):
            try:
                tup = ast.literal_eval(s)
            except (ValueError, SyntaxError):
                continue  # a line of data that merely looks like a tuple
            if isinstance(tup, tuple) and len(tup) >= 2 and isinstance(tup[0], str):
                note(tup[0], str(tup[1]))
            continue
        m = _AB_DEAD.match(line)
        if m:
            attribute(dead, loose_dead, m.group(1))
            continue
        m = _AB_PARTIAL.match(line)
        if m:
            attribute(partial, loose_partial, m.group(1))
    return AbReport(order, diffs, dead, partial, loose_dead, loose_partial)


def verify_previous_release_differential(led: Ledger) -> None:
    prev = _require_prev_binary(
        led, _R_ENGINE, _AB_VERSION, "differential", _AB_STORE,
        "previous-release differential",
    )
    if prev is None:
        return

    prev_ver = run([str(prev), "--version"]).stdout.splitlines()
    led.phase(
        f"Previous-release differential vs {prev_ver[0] if prev_ver else 'prev'} — "
        "identical fixtures, both binaries, every observable surface diffed"
    )
    # Not auto-deleted, same reasoning as stage 1: when a fingerprint differs the
    # first thing a reader wants is both binaries' parts, side by side.
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-abdiff-"))
    p = _run_interruptible(
        ["python3", "-m", _AB_MODULE, str(prev), str(rivet_bin())],
        cwd=ROOT, timeout=_ab_budget(),
        # The cleanup this waits for is the fixture DROP in the harness's own
        # `finally` — its worst case, imported.
        grace=_child_grace(_AB_CLEANUP_WORST_CASE),
        env={**_ISOLATED_STATE, "RIVET_AB_WORKDIR": str(work)},
    )
    rep = _ab_parse(p.out)
    order, diffs, dead = rep.order, rep.diffs, rep.dead

    for name in order:
        fields = diffs.get(name) or []
        why = rep.partial.get(name) or []
        extra = f" [graded less than it claims: {'; '.join(why)}]" if why else ""
        if dead.get(name):
            # A cell that compared two nothings. The harness calls this fatal and
            # so does the ledger: it is the vacuous green this stage exists for.
            led.failed(
                _R_ENGINE, _AB_VERSION, name[:16], _AB_STORE,
                f"ab-diff[{name}]: GRADED NOTHING — {'; '.join(dead[name])}. "
                "Two empty runs compare equal; this row is not evidence that the "
                "binaries agree.",
                "graded nothing",
            )
        elif fields:
            led.failed(
                _R_ENGINE, _AB_VERSION, name[:16], _AB_STORE,
                f"ab-diff[{name}]: DIFFERS from the previous release in "
                f"{', '.join(fields)} — an observable behaviour change; it needs "
                f"a verdict (intended and changelogged, or a regression){extra}",
                f"differs: {','.join(fields)}",
            )
        else:
            led.passed(
                _R_ENGINE, _AB_VERSION, name[:16], _AB_STORE,
                f"ab-diff[{name}]: identical to the previous release "
                f"(exit, DuckDB readback, files, manifest incl. fingerprints)"
                f"{extra}",
                "identical" + (" (partial)" if why else ""),
            )

    # The count row. It grades the HARNESS, not the binaries — and it is the row
    # that makes the ones above mean something.
    n = len(order)
    tail = " | ".join((p.out.strip().splitlines() or ["<no output>"])[-3:])[:300]
    reasons: list[str] = []
    if n < _AB_MIN_SCENARIOS:
        reasons.append(
            f"graded only {n} of the {_AB_MIN_SCENARIOS} scenarios it must "
            "compare (a differential that compared nothing also reports no "
            "difference)"
        )
    if not p.ok and not (any(diffs.values()) or dead):
        reasons.append(
            f"the harness exited {p.returncode} without naming a differing or "
            "dead scenario (it crashed, timed out, or the stand is down)"
        )
    if p.ok and (any(diffs.values()) or dead):
        reasons.append(
            "the harness exited 0 while its own output reports a difference or a "
            "dead cell — parser and harness disagree; trust neither until it is "
            "explained"
        )
    if rep.loose_dead:
        reasons.append(
            "the harness reported a cell that graded NOTHING and named no "
            "scenario: " + "; ".join(rep.loose_dead)[:200]
        )
    # Housekeeping notes the harness itself calls non-fatal: carried, never a
    # verdict of their own (its "could not drop the fixture" warning is one).
    housekeeping = (
        " | harness notes: " + "; ".join(rep.loose_partial)[:200]
        if rep.loose_partial else ""
    )
    differing = [k for k, v in diffs.items() if v]
    if reasons:
        led.failed(
            _R_ENGINE, _AB_VERSION, "scenarios", _AB_STORE,
            f"ab-diff: {'; '.join(reasons)} — last output: {tail}{housekeeping}",
            f"{n} scenarios, exit {p.returncode}",
        )
    elif differing or dead:
        # The count is sound; the comparison is not. This row must never read
        # "no observable difference" while a row above says DIFFERS or GRADED
        # NOTHING.
        parts = []
        if differing:
            parts.append(f"{len(differing)} DIFFER ({', '.join(differing)})")
        if dead:
            parts.append(f"{len(dead)} GRADED NOTHING ({', '.join(dead)})")
        led.failed(
            _R_ENGINE, _AB_VERSION, "scenarios", _AB_STORE,
            f"ab-diff: {n} scenarios compared, " + " and ".join(parts)
            + f" — see the rows above{housekeeping}",
            f"{n} compared, {len(differing)} differ, {len(dead)} dead",
        )
    else:
        led.passed(
            _R_ENGINE, _AB_VERSION, "scenarios", _AB_STORE,
            f"ab-diff: {n} scenarios compared against the previous release, "
            f"no observable difference{housekeeping}",
            f"{n} scenarios compared" + (" (+harness notes)" if housekeeping else ""),
        )


# ── stage 4: the FIELD SYMPTOM, replayed against both binaries ────────────────
#
# `dev/pytools/field_replay.py` reconstructs the shape of the production run
# that caught the 0.24.4 governor regression (a keyset-heavy census under a
# pool) and grades four criteria that are fixed IN THE HARNESS, before any run,
# so the verdict cannot be rationalised afterwards:
#
#   1 the OLD binary must shed at least once     (the fixture is live)
#   2 the NEW binary sheds zero on an idle source (the symptom is gone)
#   3 new makespan <= old * 1.05                  (the fix costs nothing)
#   4 identical rows per export                   (it delivers the same data)
#
# Each becomes its own ledger row, because "field replay FAILED" as a single
# cell tells a release manager nothing: criterion 3 failing is a perf call,
# criterion 4 failing is a data-loss stop-the-release, and criterion 1 failing
# means NONE of the others graded anything at all.
#
# THE FIXTURE HAS A SHELF LIFE, AND THE STAGE SAYS SO RATHER THAN DECAYING
# QUIETLY. Criterion 1 asks the PREVIOUS RELEASE to reproduce the symptom, so
# this stage is evidence only while `RIVET_PREV_RELEASE_BIN` is a release that
# CARRIES the regression (0.24.4). Measured here on 2026-08-14 with the 0.24.3
# asset — which predates the regression — criterion 1 correctly went RED (`old
# shed 0x`) and criteria 2-4 were recorded vacuous. Once the fix has shipped,
# the newest release no longer sheds either, and this stage will go permanently
# red on criterion 1 for every future gate run. That is not a false alarm to
# tune away: it is the honest report that the replay no longer replays
# anything. The two real options at that point are to pin this stage's baseline
# to the last regressing release, or to retire the fixture and replace it with
# the next symptom worth reproducing — decide it deliberately, do NOT soften
# criterion 1 into a warning, which is the vacuity this whole stage exists to
# prevent.
_FR_MODULE = "dev.pytools.field_replay"
_FR_CRITERIA = 4  # ratchet, as above: the harness fixes four.
# `  [PASS] 1 fixture is live (old must shed)  (old shed 3x)` — the criterion
# name itself contains parentheses, so the detail is split on the TWO spaces the
# harness prints before it, not on the first `(`.
_FR_CRIT = re.compile(r"^\s*\[(PASS|FAIL)\]\s+(.+?)\s{2,}\((.*)\)\s*$")

# The harness runs `len(LEG_PLAN)` legs, each capped at its OWN `LEG_TIMEOUT`
# (`RIVET_FIELD_TIMEOUT`, default 3600 s). Both numbers are IMPORTED from the
# harness rather than re-typed, so adding a leg or changing the per-leg budget
# moves this wrapper's budget with it. Slack covers preflight, the seed (24
# tables) and the report. Override outright with `RIVET_ORACLE_FIELD_TIMEOUT`.
_FR_SLACK = 1800.0


def _fr_budget() -> float:
    return _wrapper_budget(
        "RIVET_ORACLE_FIELD_TIMEOUT", _FR_LEG_TIMEOUT, len(_FR_LEG_PLAN), _FR_SLACK
    )


def _stand_globals(container: str) -> dict[str, str] | None:
    """The MySQL stand's tmp-table globals, or None if they cannot be read.

    `@@GLOBAL.x`, never `@@x` — the session copy is taken when the connection
    opens and would confirm a restore that never happened (the harness's own
    docstring records reproducing exactly that).
    """
    sql = "SELECT CONCAT_WS(',', " + ", ".join(f"@@GLOBAL.{k}" for k in _FR_FLIP) + ");"
    p = docker_exec(container, "mysql", "-uroot", "-privet", "-N", "rivet",
                    stdin=sql, timeout=60)
    lines = [l for l in p.stdout.strip().splitlines() if "," in l]
    if not p.ok or not lines:
        return None
    parts = [x.strip() for x in lines[-1].split(",")]
    if len(parts) != len(_FR_FLIP) or any(not x for x in parts):
        return None
    return dict(zip(_FR_FLIP, parts))


def _stand_risk(said_so: bool, flipped: bool, verified: bool) -> str:
    """What the CHILD'S TRANSCRIPT alone says about the shared stand:

    * `"mutated"` — the harness reported a failed restore (`STAND_MUTATED_MARKER`).
    * `"unknown"` — it reached the flip and neither verified a restore nor said it
      could not. A SIGKILLed run looks exactly like this: no marker, no `atexit`,
      no handler. This is the case that used to produce NO ROW AT ALL.
    * `"clean"` — it never reached the flip (nothing to restore), or it printed a
      VERIFIED restore (it re-read `@@GLOBAL.*` itself).

    Used when the container cannot be read back, i.e. when the transcript is the
    only evidence there is. When the container CAN be read, the read wins.
    """
    if said_so:
        return "mutated"
    if flipped and not verified:
        return "unknown"
    return "clean"


def _verify_stand_restored(led: Ledger, p: Proc) -> None:
    """Did the shared MySQL stand come back UNFLIPPED? Belt over the harness's own
    restore, and the only check that survives the harness being killed.

    Scope, stated rather than implied — this used to claim it "sees all three"
    ways the stand stays mutated, and it did so only while `docker exec … mysql`
    ANSWERS. The three ways are: the harness's restore failed (it says so with
    `STAND_MUTATED_MARKER`), the harness was SIGKILLed after ignoring SIGINT (no
    marker, no `atexit`, no handler), or the container was lost mid-run. The last
    two are precisely the ones where the read-back is ALSO likely to fail — a
    wedged MySQL wedges both — and the old code returned SILENTLY on an
    unreadable container unless the harness had printed the marker, i.e. it
    reported the case with the MOST evidence and said nothing in the case with
    NONE. An unreadable container is now classified from the transcript
    (`_stand_risk`) and always produces a row: at minimum UNKNOWN, never clean.

    It restores to DEFAULT when it finds the flip, because the alternative is a
    FAIL row that tells a release manager the stand is poisoned and leaves it
    poisoned. The row is recorded EITHER WAY: an automatic repair is not a
    reason to hide that the gate mutated a shared server and did not clean up.

    Both the read and the repair happen UNDER THE HARNESS'S OWN `StandLock`,
    non-blocking. They are the same server-wide globals `field_replay` flips, so
    an unlocked read-back could see a CONCURRENT replay's legitimate in-flight
    flip, record a false "stand mutated" FAIL, and then `SET GLOBAL … = DEFAULT`
    under a live harness — un-spilling its fixture so ITS criterion 1 fails for a
    reason unrelated to the binaries. Two wrong verdicts from one unlocked read.
    """
    container = os.environ.get("RIVET_FIELD_MYSQL_CONTAINER", "rivet-mysql-1")
    said_so = _FR_MUTATED in p.out
    risk = _stand_risk(said_so, _FR_FLIPPED in p.out, _FR_RESTORED in p.out)
    # Only the ACQUISITION is guarded: an OSError raised by the read-back itself
    # is a bug, and swallowing it here would file it under "another run holds the
    # stand", which is a different fact.
    lock = _StandLock(container, holder="release-oracle stand verify")
    try:
        lock.__enter__()
    except (_ChildFail, OSError) as e:
        # Someone else owns the stand right now. Do NOT read it (their flip is
        # legitimate) and above all do NOT repair it (that breaks their run).
        why = getattr(e, "message", None) or str(e)
        # Precomputed (never a multi-line expression inside an f-string): this
        # module must import on every Python the gate is run with, including the
        # ones that predate PEP 701.
        what = ("the harness said so" if said_so
                else "it flipped and never confirmed a restore")
        if risk == "clean":
            led.skipped(
                _R_ENGINE, _FR_VERSION, "stand", _FR_STORE,
                f"field replay: {container}'s tmp-table globals were NOT checked — another "
                f"run holds the stand lock ({why}). This replay's own transcript says it "
                "left the stand clean (it never flipped, or it verified its own restore), "
                "and reading or repairing the globals now would grade — and break — the "
                "OTHER run's live flip.",
                "stand busy — not checked",
            )
        else:
            led.failed(
                _R_ENGINE, _FR_VERSION, "stand", _FR_STORE,
                f"field replay: this replay may have left {container} MUTATED "
                f"({what}, exit {p.returncode}), and the stand could not be checked or "
                f"repaired because another run holds the lock ({why}). Check "
                f"{list(_FR_FLIP)} by hand once that run finishes.",
                f"stand {risk} — busy, not repaired",
            )
        return
    try:
        _check_stand_under_lock(led, p, container, said_so, risk)
    finally:
        lock.__exit__(None, None, None)


def _check_stand_under_lock(led: Ledger, p: Proc, container: str, said_so: bool,
                            risk: str) -> None:
    """The read-back + repair, with the stand lock already held."""
    now = _stand_globals(container)
    if now is None:
        # THE CASE WITH THE LEAST INFORMATION MUST BE THE LOUDEST. A SIGKILLed
        # harness (rc 137) prints no marker, and the wedged container that
        # provoked the kill is the same one that will not answer here — so the
        # old `if said_so: … return` reported nothing at all in exactly the
        # scenario this stage was built for.
        if risk == "clean":
            led.skipped(
                _R_ENGINE, _FR_VERSION, "stand", _FR_STORE,
                f"field replay: could not read {container}'s tmp-table globals back "
                f"(is it up?), so the stand was NOT verified. Its own transcript says the "
                f"harness left it clean — it never reached the flip, or it printed a "
                f"verified restore — so nothing here is evidence of a mutated stand "
                f"(exit {p.returncode}).",
                "stand unreadable (transcript says clean)",
            )
            return
        led.failed(
            _R_ENGINE, _FR_VERSION, "stand", _FR_STORE,
            f"field replay: {container} is UNREADABLE and this replay "
            + ("reported it could NOT restore" if said_so else
               "flipped the globals and never confirmed a restore (no restore line, no "
               "failure line — the shape of a SIGKILLed run)")
            + f" its tmp-table globals (exit {p.returncode}). The stand's state is UNKNOWN "
              f"and must be assumed left at {_FR_FLIP}: every later live MySQL run on this "
              "machine would inherit it, silently rewriting tmp-table behaviour. Check it "
              f"by hand: docker exec -i {container} mysql -uroot -privet rivet -e "
              f"\"SELECT {', '.join(f'@@GLOBAL.{k}' for k in _FR_FLIP)};\" and, if the flip "
              f"is there, {'; '.join(f'SET GLOBAL {k} = DEFAULT' for k in _FR_FLIP)};",
            "stand mutated (unverifiable)" if said_so else "stand UNKNOWN (unreadable)",
        )
        return

    stuck = {k: v for k, v in now.items() if v == _FR_FLIP[k]}
    if not stuck and not said_so:
        return

    repaired = None
    if stuck:
        sql = "; ".join(f"SET GLOBAL {k} = DEFAULT" for k in stuck) + ";"
        docker_exec(container, "mysql", "-uroot", "-privet", "-N", "rivet",
                    stdin=sql, timeout=60)
        after = _stand_globals(container)
        repaired = after is not None and not any(after[k] == _FR_FLIP[k] for k in stuck)
    led.failed(
        _R_ENGINE, _FR_VERSION, "stand", _FR_STORE,
        f"field replay: {container} was left MUTATED by the replay "
        f"({', '.join(f'{k}={v}' for k, v in (stuck or now).items())}"
        + (f"; the harness said so: exit {p.returncode}" if said_so else
           f"; the harness did not report it — exit {p.returncode}")
        + "). The gate flipped SERVER-WIDE tmp-table globals and did not put them "
          "back, so every later live MySQL test on this machine runs against a "
          "server whose tmp-table behaviour is silently rewritten. "
        + ("This stage restored them to DEFAULT." if repaired is True else
           "This stage tried to restore them to DEFAULT and could NOT — do it by "
           f"hand: docker exec -i {container} mysql -uroot -privet rivet -e "
           f"\"{'; '.join(f'SET GLOBAL {k} = DEFAULT' for k in (stuck or _FR_FLIP))};\""
           if repaired is False else
           "The harness reported a failed restore but the globals now read clean — "
           "someone or something else restored them; treat the stand as suspect."),
        "stand mutated" + (" (auto-restored)" if repaired is True else ""),
    )


def verify_field_symptom_replay(led: Ledger) -> None:
    # The stage doc above names this moment: once the fix ships, the NEWEST release
    # no longer sheds, criterion 1 goes permanently red, and the two honest options
    # are to PIN this stage's baseline to the last regressing release or to retire
    # the fixture. This is the pin, made explicit rather than by re-pointing
    # `RIVET_PREV_RELEASE_BIN` (which the perf/differential stages also read and
    # which must stay the LATEST release): `RIVET_FIELD_REPLAY_BIN` names the
    # release that CARRIES the regression — 0.24.4 for the governor fixture —
    # and without it the stage falls back to the prev binary, whose failure mode
    # criterion 1 already reports honestly.
    pinned = os.environ.get("RIVET_FIELD_REPLAY_BIN", "").strip()
    if pinned:
        prev = Path(pinned)
        if not prev.is_file():
            led.failed(_R_ENGINE, _FR_VERSION, "replay", _FR_STORE,
                       f"field replay: RIVET_FIELD_REPLAY_BIN={pinned} is not a file — "
                       "a typo here would silently grade the wrong binary",
                       "pinned replay binary missing")
            return
    else:
        prev = _require_prev_binary(
            led, _R_ENGINE, _FR_VERSION, "replay", _FR_STORE, "field symptom replay"
        )
        if prev is None:
            return

    prev_ver = run([str(prev), "--version"]).stdout.splitlines()
    led.phase(
        f"Field symptom replay vs {prev_ver[0] if prev_ver else 'prev'} — the "
        "production run's own shape, four criteria fixed before the run"
    )
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-field-"))
    p = _run_interruptible(
        ["python3", "-m", _FR_MODULE, str(prev), str(rivet_bin())],
        cwd=ROOT, timeout=_fr_budget(),
        # The cleanup this waits for is the tmp-table RESTORE its SIGINT handler
        # runs — 3 retries × (SET + read-back) — imported, not guessed.
        grace=_child_grace(_FR_CLEANUP_WORST_CASE),
        # Same cross-version state isolation as every other cell that runs two
        # binary versions — and here it is doubly load-bearing: the harness reads
        # sheds out of the SQLite `run_journal` beside each config, so a
        # process-wide `RIVET_STATE_URL` would send the journal to Postgres and
        # the fixture would read as "never shed" (criterion 1) for a reason that
        # has nothing to do with the product.
        env={**_ISOLATED_STATE, "RIVET_FIELD_WORKDIR": str(work)},
    )
    # BEFORE anything is parsed: the stand is shared, and whether it came back
    # clean is a fact about the machine every later cell runs on — not something
    # to discover from a criterion row that may not exist.
    _verify_stand_restored(led, p)

    crits = [(m.group(1), m.group(2).strip(), m.group(3))
             for m in (_FR_CRIT.match(l) for l in p.out.splitlines()) if m]
    tail = " | ".join((p.out.strip().splitlines() or ["<no output>"])[-4:])[:300]

    if not crits:
        led.failed(
            _R_ENGINE, _FR_VERSION, "criteria", _FR_STORE,
            f"field replay: the harness graded NOTHING — no criterion verdict in "
            f"its output (exit {p.returncode}). The replay needs the MySQL dev "
            "stand (container `rivet-mysql-1` at 127.0.0.1:3306) and a pool; "
            f"last output: {tail}",
            f"0 criteria, exit {p.returncode}",
        )
        return

    # Criterion 1 is the activation guard: it decides whether the other three
    # graded a reproduction or graded air. Found by its own leading number
    # rather than by position, so a reordered harness cannot silently promote a
    # different criterion into the load-bearing slot.
    live_idx = next((i for i, c in enumerate(crits) if c[1].lstrip().startswith("1")), None)
    fixture_dead = live_idx is not None and crits[live_idx][0] == "FAIL"

    for idx, (status, name, detail) in enumerate(crits, start=1):
        num = name.split()[0] if name.split()[0].isdigit() else str(idx)
        cell = f"criterion-{num}"
        if fixture_dead and idx - 1 == live_idx:
            led.failed(
                _R_ENGINE, _FR_VERSION, cell, _FR_STORE,
                f"field replay CRITERION 1 — THE FIXTURE IS DEAD: {name} "
                f"({detail}). The PREVIOUS release never reproduced the symptom "
                "in this run, so criteria 2-4 grade AIR: a green 'symptom gone' "
                "means only that nothing happened. Nothing here is evidence "
                "about the fix until this row is green. Check the MySQL dev "
                "stand, the pool, and that RIVET_PREV_RELEASE_BIN is the release "
                "that CARRIED the regression.",
                "FIXTURE DEAD — criteria 2-4 grade air",
            )
            continue
        if fixture_dead and status == "PASS":
            # Not a pass: it is an ungraded criterion wearing a pass. SKIP is the
            # ledger's word for "we could not check", which is exactly true — and
            # the run is red regardless, from criterion 1.
            led.skipped(
                _R_ENGINE, _FR_VERSION, cell, _FR_STORE,
                f"field replay [{name}]: reported PASS ({detail}) but the fixture "
                "never activated (criterion 1 FAILED) — this grades air, not the "
                "fix",
                "vacuous (fixture dead)",
            )
        elif status == "PASS":
            led.passed(_R_ENGINE, _FR_VERSION, cell, _FR_STORE,
                       f"field replay [{name}]: {detail}", detail)
        else:
            led.failed(_R_ENGINE, _FR_VERSION, cell, _FR_STORE,
                       f"field replay [{name}]: FAILED ({detail})", detail)

    # …and the same count/exit reconciliation stage 3 makes: four criteria, and
    # the harness's exit status must agree with the verdicts it printed.
    reasons = []
    if len(crits) < _FR_CRITERIA:
        reasons.append(f"only {len(crits)} of {_FR_CRITERIA} criteria were graded")
    printed_fail = any(s == "FAIL" for s, _, _ in crits)
    if p.ok and printed_fail:
        reasons.append("the harness exited 0 while printing a FAIL criterion")
    if not p.ok and not printed_fail:
        # Name the cause the harness itself named. A failed RESTORE is the one
        # non-zero exit that has nothing to do with the criteria, and reporting
        # it as "it died after printing them, or the parse missed one" sent the
        # reader hunting a parser bug while the actual fact was a poisoned
        # shared MySQL server (see the `stand` row recorded above).
        reasons.append(
            f"the harness exited {p.returncode} because it could NOT restore the "
            "shared MySQL stand's tmp-table globals — the criteria it printed are "
            "unaffected; see the `stand` row"
            if _FR_MUTATED in p.out else
            f"the harness exited {p.returncode} with every criterion PASS "
            "(it died after printing them, or the parse missed one)"
        )
    if reasons:
        led.failed(
            _R_ENGINE, _FR_VERSION, "criteria", _FR_STORE,
            f"field replay: {'; '.join(reasons)} — last output: {tail}",
            f"{len(crits)} criteria, exit {p.returncode}",
        )
    elif printed_fail:
        # Never a green summary over a red criterion — and above all never one
        # that says "fixture live" while criterion 1 says the fixture is dead.
        bad = [n.split()[0] for s, n, _ in crits if s == "FAIL"]
        led.failed(
            _R_ENGINE, _FR_VERSION, "criteria", _FR_STORE,
            f"field replay: {len(crits)} criteria graded, {len(bad)} FAILED "
            f"(criterion {', '.join(bad)}) — see the rows above",
            f"{len(crits)} graded, {len(bad)} failed",
        )
    else:
        led.passed(
            _R_ENGINE, _FR_VERSION, "criteria", _FR_STORE,
            f"field replay: all {len(crits)} criteria graded and green against "
            "the previous release (fixture live, symptom gone, no makespan "
            "cost, same data)",
            f"{len(crits)} criteria graded",
        )


# ── self-test: the wrapper's own decisions, without a stand ───────────────────
_SELFTEST_CLEANUP_CHILD = """
import signal, sys, time
def bye(signum, frame):
    print("CLEANUP RAN", flush=True)
    sys.exit(3)
signal.signal(signal.SIGINT, bye)
print("READY", flush=True)
time.sleep(60)
"""

_SELFTEST_STUBBORN_CHILD = """
import signal, subprocess, sys, time
signal.signal(signal.SIGINT, signal.SIG_IGN)
# A grandchild that INHERITS this process's stdout/stderr and outlives it: after
# the SIGKILL the pipes stay open, which is what an unbounded drain waits on.
subprocess.Popen(["/bin/sleep", "30"])
print("READY", flush=True)
time.sleep(30)
"""


def _self_test() -> int:
    """The decisions this module makes ABOUT a child harness, graded without one.

    Everything here is invisible to a live gate run: the SIGINT-vs-SIGKILL
    signal, the grace period's grammar, what the stand row says when the
    container cannot be read, what the driver announces about a comparison that
    did or did not happen. A green release-oracle run exercises none of it —
    which is why each of these shipped wrong.

    Called by `__main__ --self-test` (CI, and `tests/offline/
    release_oracle_entrypoint_guard.rs`).
    """
    import contextlib
    import io
    import tempfile
    import threading
    import time

    from ..pytools.field_replay import report as _fr_report, stand_mutated_line

    # Declared once, up front: several sections below swap a module-level name
    # for a stub, and Python requires the declaration to precede every use.
    global _run_interruptible, _stand_globals, _KILL_DRAIN

    failures: list[str] = []

    def want(name: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}{('  ' + detail) if detail else ''}")
        if not cond:
            failures.append(name)

    @contextlib.contextmanager
    def env(**kw):
        """Set/clear env vars for one block and put them back."""
        saved = {k: os.environ.get(k) for k in kw}
        try:
            for k, v in kw.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            yield
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    # ── 1. the grace period: grammar, and where the default comes from ────────
    with env(**{_GRACE_ENV: None}):
        want("an unset grace defaults to the CHILD's own cleanup worst case",
             _child_grace(_FR_CLEANUP_WORST_CASE) == _FR_CLEANUP_WORST_CASE,
             f"{_child_grace(_FR_CLEANUP_WORST_CASE):.0f}s vs field_replay's "
             f"{_FR_CLEANUP_WORST_CASE:.0f}s")
        want("…and that default really is longer than the restore it waits for "
             "(a typed 300s was not)",
             _child_grace(_FR_CLEANUP_WORST_CASE) > 300.0,
             f"{_FR_CLEANUP_WORST_CASE:.0f}s")
    with env(**{_GRACE_ENV: "0"}):
        # A duration, so "0" means zero — but it must MEAN it rather than land
        # there by way of `or 300` never firing on a truthy "0", and it must say
        # what it costs (the printed warning, captured here).
        zbuf = io.StringIO()
        with contextlib.redirect_stdout(zbuf):
            zero = _child_grace(_FR_CLEANUP_WORST_CASE)
        want("an explicit grace of 0 is honoured as zero, not as the default",
             zero == 0.0, str(zero))
        want("…and says out loud that the child will be SIGKILLed with no cleanup",
             "SIGKILLed with NO chance" in zbuf.getvalue(), zbuf.getvalue().strip()[:70])
    with env(**{_GRACE_ENV: "45"}):
        want("an explicit grace is honoured", _child_grace(_FR_CLEANUP_WORST_CASE) == 45.0)
    for bad in ("abc", "-5", "  "):
        with env(**{_GRACE_ENV: bad}):
            try:
                got = _child_grace(_FR_CLEANUP_WORST_CASE)
                ok = got == _FR_CLEANUP_WORST_CASE
            except Exception as e:  # noqa: BLE001
                ok, got = False, f"raised {type(e).__name__}: {e}"
            want(f"a malformed grace ({bad!r}) falls back instead of taking the gate down",
                 ok, str(got))

    # ── 2. the two-layer budgets: the wrapper must outlast its child ──────────
    with env(RIVET_ORACLE_FIELD_TIMEOUT=None, RIVET_ORACLE_AB_TIMEOUT=None):
        want("the field wrapper's budget exceeds the harness's own worst case",
             _fr_budget() > _FR_LEG_TIMEOUT * len(_FR_LEG_PLAN),
             f"{_fr_budget():.0f}s vs {_FR_LEG_TIMEOUT * len(_FR_LEG_PLAN):.0f}s")
        want("the ab-diff wrapper's budget exceeds the harness's own worst case "
             "(both numbers IMPORTED from it, never re-typed)",
             _ab_budget() > _AB_CASE_TIMEOUT * _AB_CHILD_RUNS,
             f"{_ab_budget():.0f}s vs {_AB_CASE_TIMEOUT * _AB_CHILD_RUNS:.0f}s "
             f"({_AB_CHILD_RUNS} runs)")

    # ── 2b. …and each STAGE hands the launcher its own child's cleanup budget ─
    #
    # Observed AT the boundary, produced by the real producer: the stage itself
    # is run with the launcher stubbed, and the `grace` it passed is read back.
    # Asserting `_child_grace(X) == X` alone would grade the helper while a call
    # site quietly passed a literal — the "correct logic on a fabricated input"
    # shape.
    real_launcher, real_globals = _run_interruptible, _stand_globals
    seen: dict[str, float] = {}
    try:
        def _stub_launcher(argv, *, timeout, env, cwd, grace):
            seen[argv[2]] = grace
            return Proc(argv, 0, "", "")

        _run_interruptible = _stub_launcher
        _stand_globals = lambda c: {k: "untouched" for k in _FR_FLIP}  # noqa: E731
        with env(RIVET_PREV_RELEASE_BIN=(shutil.which("true") or "/usr/bin/true"),
                 **{_GRACE_ENV: None}):
            with contextlib.redirect_stdout(io.StringIO()):
                verify_field_symptom_replay(Ledger(colour=False))
                verify_previous_release_differential(Ledger(colour=False))
        want("the field stage waits out field_replay's OWN restore worst case",
             seen.get(_FR_MODULE) == _FR_CLEANUP_WORST_CASE,
             f"{seen.get(_FR_MODULE)} vs {_FR_CLEANUP_WORST_CASE}")
        want("the ab-diff stage waits out ab_regression's OWN teardown worst case",
             seen.get(_AB_MODULE) == _AB_CLEANUP_WORST_CASE,
             f"{seen.get(_AB_MODULE)} vs {_AB_CLEANUP_WORST_CASE}")
    finally:
        _run_interruptible, _stand_globals = real_launcher, real_globals

    # ── 3. the criterion parser, over the harness's REAL rendered output ──────
    rows = {"fr_x": 1}

    def _leg(tag, wall, shed):
        return {"tag": tag, "exit": 0, "wall": wall, "timed_out": False,
                "backed_off": shed, "recovered": 0, "rows": rows, "stderr_tail": []}

    runs = {}
    for side, adaptive in _FR_LEG_PLAN:
        tag = f"{side}-{'on' if adaptive else 'off'}"
        runs[tag] = _leg(tag, 300, 3 if tag == "old-on" else 0)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _fr_report(runs)
    parsed = [_FR_CRIT.match(l) for l in buf.getvalue().splitlines()]
    want(f"the wrapper parses all {_FR_CRITERIA} criteria out of report()'s own output",
         sum(1 for m in parsed if m) == _FR_CRITERIA,
         f"{sum(1 for m in parsed if m)} matched")
    mutated_line = stand_mutated_line("still wrong after the SET: {'tmp_table_size': '16384'}")
    want("…and does NOT mistake the printed stand-mutated line for a criterion",
         _FR_CRIT.match(mutated_line) is None, mutated_line[:70])
    want("…while still detecting it by the marker it imports",
         _FR_MUTATED in mutated_line)

    # ── 4. the banner / footer follow the BASELINE, not the flag ──────────────
    here = Path(__file__)
    for prev, escape, must_grade in ((here, True, True), (here, False, True),
                                     (None, True, False), (None, False, False)):
        lines, footer = prev_release_banner(prev, escape)
        text = "\n".join(lines)
        if must_grade:
            want(f"a present baseline (escape={escape}) is announced as GRADING",
                 "RUN and GRADE" in text and "GIVEN UP" not in text, text.splitlines()[0])
            want(f"…and no NOT RELEASE-GRADED line closes it (escape={escape})",
                 footer is None, str(footer))
        else:
            want(f"an absent baseline (escape={escape}) says so and closes with "
                 "NOT RELEASE-GRADED",
                 footer is not None and "NOT RELEASE-GRADED" in footer,
                 str(footer)[:60])
    lines, _ = prev_release_banner(here, True)
    want("…and a baseline present UNDER the escape says the escape did not apply",
         any("escape only excuses an ABSENT one" in l for l in lines))

    # ── 5. the stand read-back: unreadable, and busy ──────────────────────────
    dead_proc = Proc(["field_replay"], 137, _FR_FLIPPED + " (prior: x)\n", "")
    clean_proc = Proc(["field_replay"], 0,
                      _FR_FLIPPED + " (prior: x)\n" + _FR_RESTORED + " (normal exit): x\n", "")
    with tempfile.TemporaryDirectory() as td:
        with env(RIVET_FIELD_LOCK=str(Path(td) / "stand.lock"),
                 RIVET_FIELD_MYSQL_CONTAINER="rivet-selftest-not-a-container"):
            real_stand_globals = _stand_globals
            reads: list[str] = []
            try:
                _stand_globals = lambda c: reads.append(c) or None  # noqa: E731
                led = Ledger(colour=False)
                with contextlib.redirect_stdout(io.StringIO()):
                    _verify_stand_restored(led, dead_proc)
                cells = [c for c in led.cells if c.scenario == "stand"]
                want("a SIGKILLed harness + an unreadable container records a LOUD row "
                     "(it used to record nothing at all)",
                     len(cells) == 1 and cells[0].status is Status.FAIL,
                     str([(c.status.value, c.detail) for c in cells]))
                want("…and the row says the stand's state is UNKNOWN, never clean",
                     bool(cells) and "UNKNOWN" in cells[0].detail,
                     cells[0].detail if cells else "<no row>")

                led = Ledger(colour=False)
                with contextlib.redirect_stdout(io.StringIO()):
                    _verify_stand_restored(led, clean_proc)
                cells = [c for c in led.cells if c.scenario == "stand"]
                want("an unreadable container after a VERIFIED restore is a SKIP, not a "
                     "false alarm",
                     len(cells) == 1 and cells[0].status is Status.SKIP,
                     str([(c.status.value, c.detail) for c in cells]))

                # …and with the stand LOCKED by someone else: read nothing, write
                # nothing, say so.
                reads.clear()
                fd = os.open(os.environ["RIVET_FIELD_LOCK"], os.O_CREAT | os.O_RDWR, 0o666)
                try:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    led = Ledger(colour=False)
                    with contextlib.redirect_stdout(io.StringIO()):
                        _verify_stand_restored(led, clean_proc)
                    cells = [c for c in led.cells if c.scenario == "stand"]
                    want("a stand held by ANOTHER run is not read (and not repaired)",
                         not reads, f"reads: {reads}")
                    want("…and that is recorded, not silently skipped",
                         len(cells) == 1 and "busy" in cells[0].detail,
                         str([(c.status.value, c.detail) for c in cells]))
                finally:
                    os.close(fd)
            finally:
                _stand_globals = real_stand_globals

    # ── 6. the SIGINT-first runner, and the bounded drain after a kill ────────
    p = _run_interruptible(["python3", "-c", _SELFTEST_CLEANUP_CHILD],
                           timeout=1.0, grace=10.0, env={}, cwd=ROOT)
    want("a timed-out child is SIGINTed, so its cleanup RUNS", "CLEANUP RAN" in p.out,
         p.out.strip().replace("\n", " | ")[:80])
    want("…and the wrapper reports 124 (cleaned up), not 137 (killed)",
         p.returncode == 124, str(p.returncode))

    real_drain, _KILL_DRAIN = _KILL_DRAIN, 2.0
    box: dict[str, object] = {}
    try:
        def _drain_case():
            box["p"] = _run_interruptible(["python3", "-c", _SELFTEST_STUBBORN_CHILD],
                                          timeout=1.0, grace=1.0, env={}, cwd=ROOT)

        # In a DAEMON thread with a deadline: the bug this guards is a HANG, and a
        # self-test that hangs reports nothing. This one fails instead.
        t = threading.Thread(target=_drain_case, daemon=True)
        t0 = time.monotonic()
        t.start()
        # Shorter than the grandchild's lifetime ON PURPOSE: an unbounded drain
        # blocks until that grandchild exits, so a deadline longer than it would
        # let the bug finish and report a pass.
        t.join(12.0)
        alive = t.is_alive()
        want("a SIGKILLed child whose grandchild holds the pipes does not hang the gate",
             not alive,
             "STILL BLOCKED in communicate()" if alive
             else f"returned in {time.monotonic() - t0:.0f}s")
        q = box.get("p")
        want("…and it reports 137 with the undrained output named",
             q is not None and q.returncode == 137 and "could NOT be drained" in q.stderr,
             (q.stderr.strip().replace("\n", " | ")[-90:]) if q is not None else "<hung>")
    finally:
        _KILL_DRAIN = real_drain

    print(f"\nregression self-test: {len(failures)} failed" if failures
          else "\nregression self-test ok")
    return 1 if failures else 0
