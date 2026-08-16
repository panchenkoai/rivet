#!/usr/bin/env python3
"""Field-shaped A/B: does the fix remove the SYMPTOM, and cost nothing?

    python3 -m dev.pytools.field_replay <old-binary> <new-binary>

The 2026-08-13 regression was not found by a test — it was found by reading a
production run's metadata: 52 exports shed workers, keyset exports ran 2-2.7x
slower than the same tables on the prior release, +1h48m makespan. So the
honest validation is to measure THOSE numbers again, on a workload shaped like
that run, with both binaries.

The shape is reconstructed from the field run's own census (154 exports:
mostly tiny `full` ones, a keyset majority at parallel 1/2/4, a few chunked),
scaled down so a pass/fail lands in minutes. No client data or names are used
— only the distribution of modes and parallelism.

ACCEPTANCE CRITERIA, fixed here before the run so the verdict cannot be
rationalised afterwards:

  1. FIXTURE IS LIVE   the OLD binary must shed at least once. Without this the
                       run reproduces nothing and the rest grades air.
  2. SYMPTOM GONE      the NEW binary sheds ZERO times on an idle source.
  3. NO REGRESSION     new makespan <= old makespan * 1.05.
  4. SAME DATA         every export delivers the same row count on both.

Criterion 1 is the one that makes the others mean anything: it is the
activation guard, and it is checked first.

════════════════════════════════════════════════════════════════════════════
Running it as a RELEASE-GATE step (what the hardening pass added, and why)
════════════════════════════════════════════════════════════════════════════

The four criteria above are untouched. What changed is everything around them,
because this harness does two things a gate step must not do carelessly: it
MUTATES A SHARED SERVER and it MEASURES WALL-CLOCK.

* **Preflight before anything is seeded or flipped**: docker, a running MySQL
  that answers as root, both binaries executable and answering `--version`, and
  `apply --pool` present on BOTH (the old binary predates nothing here, but a
  binary without the flag exits instantly, sheds zero, and fails criterion 1
  with a message about the governor that has nothing to do with the cause).

* **An exclusive LOCK, not a namespace.** Two concurrent runs must not overlap,
  and namespacing the tables cannot achieve that here for two reasons that no
  naming scheme reaches: (a) the spill fixture flips `tmp_table_size` /
  `max_heap_table_size` / `internal_tmp_mem_storage_engine`, which are
  SERVER-WIDE — run B's restore would undo run A's flip and quietly make A's
  fixture inert (criterion 1 then fails for a reason nobody can see); and (b)
  criterion 3 is a WALL-CLOCK comparison, which a co-tenant load invalidates
  even if every table were unique. So the lock is on the STAND, `flock`-based
  and therefore released by the kernel when the process dies — no stale
  lockfile, no age heuristic, no clock. The tables keep fixed names because the
  lock already guarantees exclusivity, and a fixed name is what makes a
  leftover table identifiable.
  Known gap, stated rather than hidden: `governor_ab.py` and
  `tests/live/live_governor.rs` flip the SAME globals and do not yet take this
  lock. Teaching them to take it is the follow-up that closes the class.

* **The restore is the contract, not a courtesy.** A gate that leaves the
  server mutated is worse than one that fails: every later run on that stand
  inherits the flip, and — the part that bites — the NEXT run captures the
  leaked value as the "prior" it will restore, so the leak becomes permanent.
  (Found exactly that on 2026-08-14: the dev stand was sitting at
  MEMORY/16384/16384 from an interrupted run.) So the restore now survives an
  exception (`__exit__`), SIGINT/SIGTERM/SIGHUP (handlers installed on entry),
  and any other interpreter exit (`atexit`); it is idempotent, RETRIED, and
  VERIFIED by reading the globals back; a restore that cannot be verified turns
  the run red, prints the exact SQL to run by hand, and emits the machine-
  readable `STAND_MUTATED_MARKER` so the release-oracle wrapper records it as a
  poisoned STAND rather than as "the parse missed a criterion".
  `SIGKILL` remains the one hole, and it is now closed from BOTH sides rather
  than only named: the wrapper that used to send it
  (`dev/release_oracle/regression.py`) sends SIGINT and waits out a grace period
  first — SIGINT is caught by the handler below — and after the child returns
  for ANY reason it reads `@@GLOBAL.*` back off the stand itself, restoring to
  DEFAULT and failing the cell if the flip is still there. A SIGKILL that
  happens anyway (the gate's own process dying, `kill -9` by hand) is still
  detected on the NEXT run (prior == FLIP) and restored to DEFAULT rather than
  perpetuated.

* **`@@GLOBAL.x`, never `@@x`.** `@@tmp_table_size` is the SESSION copy, taken
  from the global when the connection opened — so a verification issued in the
  same session as the `SET GLOBAL` reads the value it had BEFORE the write and
  cheerfully confirms a restore that did not happen. (Reproduced on the stand
  while writing this.)

* **A measurement that cannot be taken FAILS**: a missing state DB or an
  unreadable `run_journal` used to return `(0, 0)`, which is indistinguishable
  from "the governor behaved" and is exactly how criterion 2 passes vacuously.
  Every child also gets `RIVET_STATE_URL=""`, so a caller that exports a shared
  Postgres state DB (the release oracle does, process-wide) cannot send the
  journal somewhere this harness will never look.
"""

from __future__ import annotations

import atexit
import fcntl
import json
import os
import re
import shutil
import signal
import sqlite3
import sys
import time
from pathlib import Path

from .shell import Fail, have, main as shell_main, run, tcp_open

MYSQL = os.environ.get("RIVET_FIELD_MYSQL_URL", "mysql://rivet:rivet@127.0.0.1:3306/rivet")
MYSQL_CONTAINER = os.environ.get("RIVET_FIELD_MYSQL_CONTAINER", "rivet-mysql-1")
WORK = Path(os.environ.get("RIVET_FIELD_WORKDIR", "/tmp/rivet-field-replay"))
POOL_SLOTS = "5"

# Generous: a leg of this fixture is minutes, and a timeout must mean "hung",
# never "slow".
#
# A timed-out leg surfaces as returncode 124 — and, since 2026-08-16, as a
# `timed_out` flag that POISONS every criterion which consumes that leg. The
# comment here used to claim a timeout "fails criterion 3 loudly", which is true
# only when the NEW leg is the one that dies: criterion 3 is `new <= old * 1.05`,
# so a killed OLD leg hands it a 3600 s ceiling that any real new run beats, and
# the wall-clock FLOOR of a killed process is then graded as a measurement
# ("400.0s vs 3600.0s — PASS"). Criterion 2 has the same shape from the other
# side: a killed NEW leg sheds no more, so "new shed 0x" passes vacuously. Both
# now report UNGRADED and FAIL. Criterion 1 is the exception, and deliberately:
# it asks for a POSITIVE observation (the old binary shed at least once), which
# a truncated leg can only under-report, never manufacture.
LEG_TIMEOUT = float(os.environ.get("RIVET_FIELD_TIMEOUT") or 3600)

# The legs, in the order they run and the order `report` prints them. ONE list,
# so the leg COUNT is derivable rather than re-typed: the release-oracle wrapper
# (dev/release_oracle/regression.py) sizes its own budget as
# `len(LEG_PLAN) * LEG_TIMEOUT + slack` by importing these two names. Before that
# it read the SAME `RIVET_FIELD_TIMEOUT` for a single whole-harness budget, so
# the wrapper always expired first (X vs 4X) and SIGKILLed the harness — the one
# signal the restore contract below cannot survive.
LEG_PLAN = (("old", False), ("old", True), ("new", False), ("new", True))

# Empty means "SQLite beside the config" — see the state note in the docstring.
# `run()` merges over os.environ, so CLEARING an inherited value needs the empty
# string, not a missing key.
CHILD_ENV = {"RUST_LOG": "warn", "RIVET_GOVERNOR_INTERVAL_MS": "200",
             "PATH": "/usr/bin:/bin:/usr/local/bin", "RIVET_STATE_URL": ""}

# (label, count, rows, mode, parallel) — the field census, scaled.
# The heavy keyset shapes are what the regression hit, so they carry the weight.
# NOTE on fidelity, stated rather than glossed: the field's KEYSET exports are
# what the regression hit, but `chunk_by_key:` requires the `table:` shortcut
# (the planner must see a unique index), so a keyset export cannot carry the
# `SELECT DISTINCT` that makes a read provably spill. The spill condition is
# therefore carried by chunked exports at the SAME parallelism — the same
# mechanism the buggy governor fed on (its counter is server-wide, not
# per-runner) — while real keyset exports keep the runner mix in the pool.
CENSUS = [
    ("heavy", 6, 60_000, "spill", 4),
    ("mid", 4, 20_000, "spill", 2),
    ("keyset", 3, 40_000, "keyset", 4),
    ("small", 8, 500, "full", None),
]


def sh(cmd, **kw):
    kw.setdefault("timeout", LEG_TIMEOUT)
    return run(cmd, **kw)


# How long ONE `docker exec … mysql` round-trip may take. Named (not a literal
# at the call site) because the restore's worst case is built from it, and the
# release-oracle wrapper's SIGINT grace period is built from THAT — see
# RESTORE_WORST_CASE below.
MYSQL_ROOT_TIMEOUT = 600.0


def mysql_root(sql: str, *, what: str = "mysql") -> str:
    """SQL as root inside the dev MySQL container.

    Raises on failure. The version this replaced ignored the exit status, so a
    failed seed, a failed flip and a failed RESTORE were all indistinguishable
    from success — and the restore is the one that leaves the stand poisoned.
    """
    p = run(["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-privet",
             "-N", "rivet"], stdin=sql, timeout=MYSQL_ROOT_TIMEOUT)
    if not p.ok:
        tail = [ln for ln in (p.stderr or "").splitlines() if "Using a password" not in ln]
        raise Fail(f"{what}: mysql root command failed on {MYSQL_CONTAINER}: "
                   f"{tail[-1] if tail else p.returncode}", hint=f"SQL was: {sql[:200]}")
    return p.stdout.strip()


def tables():
    for label, n, rows, mode, par in CENSUS:
        for i in range(n):
            yield f"fr_{label}_{i}", rows, mode, par


def seed():
    print("== seeding the field-shaped fixture")
    stmts = ["SET SESSION cte_max_recursion_depth = 1000000;"]
    for name, rows, _, _ in tables():
        stmts.append(f"DROP TABLE IF EXISTS {name};")
        stmts.append(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload VARCHAR(512)) ENGINE=InnoDB;")
        stmts.append(
            f"INSERT INTO {name} (id, payload) WITH RECURSIVE s(n) AS "
            f"(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < {rows}) "
            f"SELECT n, REPEAT('x', 512) FROM s;")
    mysql_root("\n".join(stmts), what="seed")
    # A half-seeded fixture surfaces later as a row-count difference attributed
    # to the BINARIES; caught here it is still a fixture bug.
    counts = mysql_root(
        " UNION ALL ".join(f"SELECT '{n}', COUNT(*) FROM {n}" for n, _, _, _ in tables()) + ";",
        what="seed verify")
    got = dict(line.split("\t") for line in counts.splitlines() if "\t" in line)
    wrong = {n: (got.get(n), rows) for n, rows, _, _ in tables() if got.get(n) != str(rows)}
    if wrong:
        raise Fail(f"fixture seed is wrong on {len(wrong)} table(s): "
                   + ", ".join(f"{n} has {a}, expected {b}" for n, (a, b) in list(wrong.items())[:5]))
    print(f"   {sum(1 for _ in tables())} tables, verified row counts")


# ── the server-wide flip, and its restore ──────────────────────────────────────
# What is flipped, and to what. Read as: "prior == FLIP[k]" means a previous run
# leaked this one, so its true prior is unknowable and DEFAULT is the only honest
# restore target.
FLIP = {
    "internal_tmp_mem_storage_engine": "MEMORY",
    "tmp_table_size": "16384",
    "max_heap_table_size": "16384",
}
_GLOBALS = tuple(FLIP)

# Set when a restore could not be verified. `main` returns non-zero on it, so a
# poisoned stand can never be reported as a passing gate step.
RESTORE_FAILED: list[str] = []

# The machine-readable line `main` prints when the stand was left mutated. The
# release-oracle wrapper imports this NAME (never a re-typed copy of the text)
# and turns it into its own FAIL row, because the alternative reading of "exit 1
# with four green criteria" is "it died after printing them, or the parse missed
# one" — a parser bug, which is what the wrapper used to report while the actual
# fact was a poisoned shared MySQL server.
STAND_MUTATED_MARKER = "!! STAND LEFT MUTATED:"

# The other two machine-readable facts about the stand, printed by the flip and
# by a VERIFIED restore. The release-oracle wrapper imports these NAMES (never a
# re-typed copy) to tell three cases apart when it cannot read the container back
# itself: the harness never reached the flip (the stand cannot be mutated), it
# flipped and verified a restore (probably clean, unconfirmable), or it flipped
# and neither restored nor said so — a SIGKILLed run, which is exactly the case
# the wrapper used to report as nothing at all.
STAND_FLIPPED_MARKER = "== flipping tmp-table globals"
STAND_RESTORED_MARKER = "== tmp-table globals restored"

# The restore's own worst case, in seconds, DERIVED from the numbers that produce
# it: each attempt is a `SET GLOBAL` round-trip plus a read-back (two
# `mysql_root` calls), followed by a backoff sleep.
#
# It exists because the release-oracle wrapper's SIGINT grace period must be at
# least this long: the wrapper sends SIGINT, this class's handler runs `restore`,
# and a SIGKILL before it finishes leaves the stand PARTIALLY restored while the
# wrapper's transcript asserts "the child's cleanup did NOT run". A grace period
# TYPED as a round number (it was 300 s, against a ~3600 s worst case) is a
# guess about someone else's code; this is the number itself. The condition that
# makes the restore slow — a wedged MySQL — is precisely the condition that makes
# the wrapper's timeout fire, so the two meet in every real incident.
RESTORE_ATTEMPTS = 3
RESTORE_BACKOFF = 1.0
RESTORE_WORST_CASE = RESTORE_ATTEMPTS * (2 * MYSQL_ROOT_TIMEOUT + RESTORE_BACKOFF)


def stand_mutated_line(reason: str) -> str:
    """The exact line `main` prints when the stand was left mutated.

    A function rather than an inline f-string so the release-oracle's parser can
    be graded against the RENDERED line — the thing it actually reads — instead
    of against `STAND_MUTATED_MARKER`, the constant it already imports. (The old
    self-test asserted the CONSTANT contains no `(`, while the line it stands for
    contains two: it was grading a proxy, and the proxy passed.)
    """
    return (f"  {STAND_MUTATED_MARKER} {MYSQL_CONTAINER} still carries this run's tmp-table "
            f"flip ({reason}) — see the restore banner above for the exact SQL")


def read_globals() -> dict[str, str]:
    """The GLOBAL values — `@@GLOBAL.x`, never `@@x`.

    `@@x` is the SESSION copy, initialised from the global when the connection
    opened. Each call here opens a fresh connection so the two happen to agree,
    but a verification that ever shares a session with its own `SET GLOBAL`
    would read the pre-write value and confirm a restore that never happened.
    Verified on the stand: after `SET GLOBAL tmp_table_size = DEFAULT`, `@@tmp_
    table_size` still answered 16384 while `@@GLOBAL.tmp_table_size` answered
    16777216.
    """
    row = mysql_root("SELECT CONCAT_WS(',', "
                     + ", ".join(f"@@GLOBAL.{v}" for v in _GLOBALS) + ");",
                     what="read globals")
    parts = [p.strip() for p in row.split(",")]
    if len(parts) != len(_GLOBALS) or any(p == "" for p in parts):
        raise Fail(f"could not read the tmp-table globals from {MYSQL_CONTAINER}: {row!r}",
                   hint="nothing was flipped — the server is untouched")
    return dict(zip(_GLOBALS, parts))


class TmpTableGlobals:
    """Force the exports' own reads to spill — the condition the buggy governor
    mistook for source pressure. Restored on exit, on an exception, on a signal,
    and at interpreter exit; the restore is idempotent, retried and VERIFIED.

    The three ways a restore gets skipped, and how each is closed:

      * an exception in the body      -> `__exit__` (Python already guarantees it)
      * SIGINT / SIGTERM / SIGHUP     -> handlers installed in `__enter__`, which
                                         restore and then re-raise the signal with
                                         its ORIGINAL disposition, so the exit
                                         status still reports the signal
      * any other interpreter exit    -> `atexit`, which also covers a SIGINT that
                                         lands DURING the `__exit__` restore

    SIGKILL cannot be caught. That is the one hole; a leak is then detected on
    the NEXT run (prior == FLIP) and restored to DEFAULT rather than perpetuated.
    """

    def __enter__(self) -> "TmpTableGlobals":
        self.prior = read_globals()
        self.done = False
        # A prior that already equals our flip value is a LEAK from an
        # interrupted run, not somebody's deliberate setting: restoring it would
        # make the leak permanent. DEFAULT is the only honest target.
        self.target = {}
        leaked = []
        for k, v in self.prior.items():
            if v == FLIP[k]:
                self.target[k] = "DEFAULT"
                leaked.append(k)
            else:
                self.target[k] = v
        if leaked:
            print(f"   ! {MYSQL_CONTAINER} was ALREADY flipped on {', '.join(leaked)} "
                  f"— a previous run did not restore. Those will be restored to DEFAULT, "
                  f"not to the leaked value.")
        print(f"{STAND_FLIPPED_MARKER} (prior: "
              + ", ".join(f"{k}={v}" for k, v in self.prior.items()) + ")", flush=True)

        self._prior_handlers = {}
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            self._prior_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self._on_signal)
        atexit.register(self._atexit)

        mysql_root("; ".join(f"SET GLOBAL {k}={v}" for k, v in FLIP.items()) + ";",
                   what="flip globals")
        now = read_globals()
        if now != FLIP:
            # Never run the legs against a fixture that is not actually spilling:
            # the governor would then be graded on a signal that never appears.
            self.restore(why="the flip did not take")
            raise Fail(f"the tmp-table flip did not take on {MYSQL_CONTAINER} "
                       f"(wanted {FLIP}, got {now})",
                       hint="does the root user still have SUPER / SYSTEM_VARIABLES_ADMIN?")
        return self

    def __exit__(self, *_exc) -> None:
        try:
            self.restore(why="normal exit" if _exc[0] is None else f"exception {_exc[0].__name__}")
        finally:
            for sig, handler in self._prior_handlers.items():
                signal.signal(sig, handler)

    # ── the restore, from every direction ──
    def _atexit(self) -> None:
        self.restore(why="interpreter exit")

    def _on_signal(self, signum, _frame) -> None:
        name = signal.Signals(signum).name
        print(f"\n== {name} — restoring the tmp-table globals before exiting", flush=True)
        self.restore(why=f"signal {name}")
        # Re-raise with the ORIGINAL disposition so the exit status still says
        # "killed by <signal>" (and a SIGINT still raises KeyboardInterrupt for
        # any enclosing `finally`), rather than being swallowed by this handler.
        signal.signal(signum, self._prior_handlers.get(signum, signal.SIG_DFL))
        os.kill(os.getpid(), signum)

    def restore(self, *, why: str) -> None:
        """Put the globals back and PROVE it. Idempotent: `done` is set only
        after a verified restore, so a call interrupted half-way is retried by
        whichever of the three paths fires next."""
        if self.done:
            return
        sql = "; ".join(f"SET GLOBAL {k} = {v}" for k, v in self.target.items()) + ";"
        last = ""
        for _attempt in range(RESTORE_ATTEMPTS):
            try:
                mysql_root(sql, what="restore globals")
                now = read_globals()
            except Exception as e:  # noqa: BLE001 — a restore retries on ANY failure
                last = str(e)
                time.sleep(RESTORE_BACKOFF)
                continue
            # For a DEFAULT target the value is whatever this server's default
            # is; all that can be asserted is that it is no longer OUR flip.
            bad = {k: now[k] for k, want in self.target.items()
                   if (now[k] == FLIP[k] if want == "DEFAULT" else now[k] != want)}
            if not bad:
                self.done = True
                print(f"{STAND_RESTORED_MARKER} ({why}): "
                      + ", ".join(f"{k}={v}" for k, v in now.items()), flush=True)
                return
            last = f"still wrong after the SET: {bad}"
            time.sleep(RESTORE_BACKOFF)
        RESTORE_FAILED.append(last)
        print("\n" + "!" * 78, flush=True)
        print(f"  COULD NOT RESTORE {MYSQL_CONTAINER}'s tmp-table globals ({why}): {last}")
        print( "  The stand is LEFT MUTATED. Every later run on it inherits the flip.")
        print(f"  Restore by hand:\n    docker exec -i {MYSQL_CONTAINER} mysql -uroot -privet "
              f"rivet -e \"{sql}\"")
        print("!" * 78 + "\n", flush=True)


# ── the exclusive lock on the stand (see the docstring's Concurrency note) ─────
class StandLock:
    """`flock` on a file named after the STAND, taken non-blocking.

    flock is held by the open file description and released by the KERNEL when
    the process dies, so there is no stale lock to age out and no clock to
    compare — the same reason the orphan-GC rule prefers a lifecycle record over
    a freshness timer.
    """

    def __init__(self, container: str, *, holder: str = "field_replay") -> None:
        self.path = Path(os.environ.get("RIVET_FIELD_LOCK")
                         or f"/tmp/rivet-stand-{container}.lock")
        # Written into the lock file so a contender's error names WHO holds it.
        # Parameterised because this harness is no longer the only taker: the
        # release-oracle's stand read-back takes the same lock before it reads or
        # repairs `@@GLOBAL.*` (it used to do both with no lock at all, and could
        # therefore un-flip a CONCURRENT replay's fixture mid-run).
        self.holder = holder

    def __enter__(self) -> "StandLock":
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o666)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            holder = ""
            try:
                holder = os.read(self.fd, 256).decode().strip()
            except OSError:
                pass
            os.close(self.fd)
            raise Fail(
                f"another run holds the exclusive lock on {MYSQL_CONTAINER} "
                f"({self.path}{': ' + holder if holder else ''})",
                hint="this harness flips SERVER-WIDE globals and measures wall-clock, so two "
                     "runs cannot share the stand: wait for it, or point RIVET_FIELD_MYSQL_"
                     "CONTAINER/RIVET_FIELD_MYSQL_URL at a stand of your own",
            )
        os.ftruncate(self.fd, 0)
        os.write(self.fd, f"pid {os.getpid()} {self.holder}\n".encode())
        return self

    def __exit__(self, *_exc) -> None:
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)


def write_config(work: Path, adaptive: bool) -> Path:
    exports = []
    for name, _, mode, par in tables():
        lines = [f"  - name: {name}"]
        if mode == "spill":
            # DISTINCT makes every chunk materialise the derived table, which
            # with the shrunken tmp-table globals provably reaches disk — the
            # own-read exhaust the buggy governor mistook for source pressure.
            lines += [f'    query: "SELECT DISTINCT id, payload FROM {name}"',
                      "    mode: chunked", "    chunk_column: id",
                      "    chunk_size: 10000"]
        elif mode == "keyset":
            lines += [f"    table: {name}", "    mode: chunked",
                      "    chunk_by_key: id", "    chunk_size: 10000"]
        else:
            lines += [f"    table: {name}", "    mode: full"]
        if par:
            lines.append(f"    parallel: {par}")
        lines += ["    format: parquet",
                  f"    destination: {{ type: local, path: {work / 'out' / name} }}"]
        exports.append("\n".join(lines))
    cfg = work / "cfg.yaml"
    cfg.write_text(
        f"source:\n  type: mysql\n  url: \"{MYSQL}\"\n  tuning:\n"
        f"    adaptive: {str(adaptive).lower()}\n    min_parallel: 1\n    batch_size: 500\n"
        f"exports:\n" + "\n".join(exports) + "\n")
    return cfg


def _state_db(work: Path, tag: str) -> Path:
    """The run's own state DB, or a hard failure naming why it is not there.

    Returning zeros for a missing DB is how criterion 2 ("the new binary sheds
    zero") passes without a single measurement — a run that never started sheds
    zero too. CHILD_ENV clears RIVET_STATE_URL so the state cannot land in a
    Postgres this harness will never read; this is the check that notices if
    that ever stops working.
    """
    db = work / ".rivet_state.db"
    if not db.exists():
        raise Fail(f"leg {tag}: no state DB at {db} — nothing to measure",
                   hint="the run never reached the state store (check its stderr above); "
                        "if RIVET_STATE_URL is set in this environment it should have been "
                        "cleared for the child")
    return db


def journal_sheds(state_db: Path, tag: str) -> tuple[int, int]:
    """(backed off, recovered) across every export in the run."""
    try:
        db = sqlite3.connect(state_db)
        rows = list(db.execute("SELECT journal_json FROM run_journal"))
    except sqlite3.Error as e:
        raise Fail(f"leg {tag}: cannot read run_journal from {state_db}: {e}",
                   hint="the shed count is the whole verdict — an unreadable journal is a "
                        "failed measurement, not a shed count of zero") from e
    back = rec = 0
    for (js,) in rows:
        for e in json.loads(js).get("entries", []):
            pa = e.get("event", {}).get("ParallelismAdjusted")
            if pa:
                back += "backed off" in pa["reason"]
                rec += "recovered" in pa["reason"]
    return back, rec


def delivered(state_db: Path, tag: str) -> dict:
    try:
        db = sqlite3.connect(state_db)
        return dict(db.execute(
            "SELECT export_name, total_rows FROM export_metrics WHERE status='success'"))
    except sqlite3.Error as e:
        raise Fail(f"leg {tag}: cannot read export_metrics from {state_db}: {e}") from e


def leg_tag(side: str, adaptive: bool) -> str:
    return f"{side}-{'on' if adaptive else 'off'}"


def run_leg(binary: Path, side: str, adaptive: bool) -> dict:
    """One leg. `side` is passed in rather than derived from the binary path, so
    pointing the harness at the same binary twice (its own self-test) still gives
    each leg its own work dir instead of the second overwriting the first."""
    tag = leg_tag(side, adaptive)
    work = WORK / tag
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    cfg = write_config(work, adaptive)
    print(f"== leg {tag}: {binary}", flush=True)
    t0 = time.monotonic()
    r = sh([str(binary), "apply", str(cfg), "--pool", POOL_SLOTS], env=CHILD_ENV, cwd=work)
    wall = time.monotonic() - t0
    timed_out = r.returncode == 124
    if timed_out:
        print(f"   ! leg {tag} TIMED OUT after {LEG_TIMEOUT:.0f}s — its wall time is a floor, "
              "not a measurement, and every criterion that reads this leg is UNGRADED")
    db = _state_db(work, tag)
    back, rec = journal_sheds(db, tag)
    # `timed_out` is CARRIED, not just printed: a note in the log is invisible to
    # the verdict, and the verdict is what the release gate records.
    return {"tag": tag, "exit": r.returncode, "wall": wall, "timed_out": timed_out,
            "backed_off": back, "recovered": rec, "rows": delivered(db, tag),
            "stderr_tail": r.stderr.strip().splitlines()[-4:]}


# ── preflight ──────────────────────────────────────────────────────────────────
def _binary(label: str, raw: str, problems: list[str]) -> Path:
    p = Path(raw).expanduser().resolve()
    if not p.is_file():
        problems.append(f"{label} binary {p} is not a file")
        return p
    if not os.access(p, os.X_OK):
        problems.append(f"{label} binary {p} is not executable")
        return p
    v = run([str(p), "--version"], timeout=60)
    if not v.ok:
        problems.append(f"{label} binary {p} does not run: {(v.stderr or v.stdout).strip()[:120]}")
        return p
    print(f"   {label}: {p} ({v.stdout.strip()})")
    if "--pool" not in run([str(p), "apply", "--help"], timeout=60).out:
        problems.append(f"{label} binary {p} has no `apply --pool` — every leg would exit "
                        "immediately, shed nothing, and fail criterion 1 for the wrong reason")
    return p


def preflight(old_raw: str, new_raw: str) -> tuple[Path, Path]:
    """Everything this harness needs, checked BEFORE it takes the lock, seeds a
    table or touches a server-wide global."""
    print("== preflight")
    problems: list[str] = []
    if not have("docker"):
        problems.append("`docker` is not on PATH (the dev stand runs in docker)")
    old, new = _binary("old", old_raw, problems), _binary("new", new_raw, problems)
    if old == new:
        print("   ! old and new are the SAME binary — criterion 1 (the old binary must shed) "
              "then also grades the new one; this is a self-test, not a release comparison")

    if have("docker"):
        names = run(["docker", "ps", "--format", "{{.Names}}"], timeout=60).stdout.split()
        if MYSQL_CONTAINER not in names:
            problems.append(f"container {MYSQL_CONTAINER} is not running "
                            f"(docker compose up -d mysql; or set RIVET_FIELD_MYSQL_CONTAINER)")
        else:
            # Up is not ready: a MySQL still recovering accepts `docker exec` and
            # answers nothing, which used to surface as an empty prior-globals
            # read half-way through the flip.
            probe = run(["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-privet",
                         "-N", "rivet"], stdin="SELECT 1;", timeout=60)
            if probe.stdout.strip() != "1":
                problems.append(f"{MYSQL_CONTAINER} is up but not answering as root yet "
                                f"({(probe.stderr or probe.stdout).strip().splitlines()[-1:]})")
            else:
                missing = [v for v in _GLOBALS if not run(
                    ["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-privet",
                     "-N", "rivet"], stdin=f"SELECT @@GLOBAL.{v};", timeout=60).ok]
                if missing:
                    problems.append(f"{MYSQL_CONTAINER} has no {', '.join(missing)} — the spill "
                                    "fixture cannot be armed on this MySQL version")
            m = re.search(r"@([^:/]+):(\d+)/", MYSQL)
            if m and not tcp_open(m.group(1), int(m.group(2))):
                problems.append(f"nothing is listening on {m.group(1)}:{m.group(2)} — {MYSQL} is "
                                "not reachable the way rivet will reach it")
            elif m and f":{m.group(2)}" not in run(["docker", "port", MYSQL_CONTAINER],
                                                   timeout=60).stdout:
                problems.append(f"{MYSQL_CONTAINER} does not publish port {m.group(2)} — the "
                                f"fixture would be seeded into a different server than {MYSQL} "
                                "points at, and the globals flipped on a third")
    if problems:
        raise Fail(f"preflight failed — {len(problems)} missing prerequisite(s):\n"
                   + "\n".join(f"      - {p}" for p in problems),
                   hint="nothing was seeded, no global was flipped, no lock was taken")
    print(f"   docker present, {MYSQL_CONTAINER} answering as root, both binaries answer "
          "--version and have `apply --pool`")
    return old, new


def _usage() -> str:
    return ("usage: python3 -m dev.pytools.field_replay <old-binary> <new-binary>\n"
            "         python3 -m dev.pytools.field_replay --self-test\n"
            "  env: RIVET_FIELD_MYSQL_URL / RIVET_FIELD_MYSQL_CONTAINER /\n"
            "       RIVET_FIELD_WORKDIR / RIVET_FIELD_LOCK / RIVET_FIELD_TIMEOUT")


# ── self-test: the verdict logic, without a stand ──────────────────────────────
def _self_test() -> int:
    """`report()`'s handling of a TIMED-OUT leg, over synthetic legs.

    No docker, no MySQL, no binaries — this grades the one part of the harness
    that decides the release verdict, and it is here because the bug it guards
    was invisible to every live run: a leg killed at `LEG_TIMEOUT` reports
    numbers that are FLOORS, and each criterion is directional, so the floor
    lands on the PASSING side. Before the fix, an old-leg timeout printed
    `[PASS] 3 no makespan regression … (400.0s vs 3600.0s)` — a wall-clock
    ceiling graded as a measurement, and recorded as a green release-gate row.

    Its call site is `tests/offline/release_oracle_entrypoint_guard.rs`, so it
    runs in the offline suite rather than waiting for someone to remember it.
    """
    import contextlib
    import io

    rows = {"fr_heavy_0": 60_000}

    def leg(tag, wall, backed_off, timed_out=False):
        return {"tag": tag, "exit": 124 if timed_out else 0, "wall": wall,
                "timed_out": timed_out, "backed_off": backed_off, "recovered": 0,
                "rows": rows, "stderr_tail": []}

    def graded(old_on_wall, old_on_shed, new_on_wall, new_on_shed, *,
               old_timed_out=False, new_timed_out=False):
        # Built from LEG_PLAN, not from a typed list of four tags: a fifth leg
        # must extend this fixture rather than break it. (The `want` this
        # replaces asserted `len(LEG_PLAN) == 4` against a literal sitting beside
        # it — it graded nothing, and went RED on exactly the change the derived
        # leg count exists to allow.)
        runs = {}
        for side, adaptive in LEG_PLAN:
            tag = leg_tag(side, adaptive)
            if tag == "old-on":
                runs[tag] = leg(tag, old_on_wall, old_on_shed, old_timed_out)
            elif tag == "new-on":
                runs[tag] = leg(tag, new_on_wall, new_on_shed, new_timed_out)
            else:
                runs[tag] = leg(tag, 300, 0)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = report(runs)
        out = buf.getvalue()
        verdicts = {}
        details = {}
        for line in out.splitlines():
            s = line.strip()
            if s.startswith("[PASS] ") or s.startswith("[FAIL] "):
                verdicts[s[7:8]] = s[1:5]
                details[s[7:8]] = s
        return rc, verdicts, out, details

    failures = []

    def want(name, cond, detail=""):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}{('  ' + detail) if detail else ''}")
        if not cond:
            failures.append(name)

    rc, v, out, d = graded(3600, 3, 400, 0, old_timed_out=True)
    want("a timed-out OLD leg does not let criterion 3 pass on its floor",
         v.get("3") == "FAIL", f"criterion 3 = {v.get('3')}")
    want("…and criterion 1 (a POSITIVE observation) still passes",
         v.get("1") == "PASS", f"criterion 1 = {v.get('1')}")
    want("…and the harness exits non-zero", rc != 0, f"rc={rc}")

    rc, v, out, d = graded(3000, 3, 3600, 0, new_timed_out=True)
    want("a timed-out NEW leg does not let criterion 2 ('sheds 0') pass vacuously",
         v.get("2") == "FAIL", f"criterion 2 = {v.get('2')}")

    rc, v, out, d = graded(3000, 3, 400, 0)
    want("control: with no timeout every criterion is graded and green",
         rc == 0 and set(v.values()) == {"PASS"}, f"rc={rc} {v}")

    # A timed-out OLD leg that shed NOTHING. The note on criterion 1 asserts an
    # OBSERVATION ("a shed was observed before it did"), so it must be gated on
    # the observation, not on the timeout alone — the release-oracle copies this
    # detail verbatim into a row headed "CRITERION 1 — THE FIXTURE IS DEAD", and
    # printed both halves at once: `old shed 0x [leg timed out; a shed was
    # observed before it did]`.
    rc, v, out, d = graded(3600, 0, 400, 0, old_timed_out=True)
    want("a timed-out OLD leg that shed NOTHING fails criterion 1",
         v.get("1") == "FAIL", f"criterion 1 = {v.get('1')}")
    want("…and its detail does NOT claim a shed was observed",
         "a shed was observed" not in d.get("1", ""), d.get("1", "<no criterion 1 line>"))

    # NOT here: whether the wrapper's criterion regex parses this harness's
    # output (and refuses `stand_mutated_line`). That regex lives in
    # `dev/release_oracle/regression.py`, which imports this module — re-typing a
    # copy of it here would grade a proxy, which is exactly what the `want` this
    # replaces did (it asserted `"(" not in STAND_MUTATED_MARKER`, a property of
    # the CONSTANT, while the line it stands for contains two parentheses). It is
    # graded from the other side, over `report()`'s REAL rendered output, in
    # `regression._self_test`.

    print(f"\nself-test: {len(failures)} failed" if failures else "\nself-test ok")
    return 1 if failures else 0


def main() -> int:
    # Line-buffered, so this harness's stdout and `Fail`'s stderr interleave in
    # the order they were written when a CI job captures both into one log.
    sys.stdout.reconfigure(line_buffering=True)
    argv = sys.argv[1:]
    if any(a in ("-h", "--help") for a in argv):
        print(__doc__)
        print(_usage())
        return 0
    if argv == ["--self-test"]:
        return _self_test()
    if len(argv) != 2:
        raise Fail("two binaries are required", hint=_usage())
    old, new = preflight(argv[0], argv[1])

    WORK.mkdir(parents=True, exist_ok=True)
    binaries = {"old": old, "new": new}
    with StandLock(MYSQL_CONTAINER):
        seed()
        with TmpTableGlobals():
            runs = {}
            for side, adaptive in LEG_PLAN:
                r = run_leg(binaries[side], side, adaptive)
                runs[r["tag"]] = r
        rc = report(runs)
    if RESTORE_FAILED:
        # Machine-readable, and NOT in the `[PASS]/[FAIL] <criterion>  (detail)`
        # shape the wrapper parses as a criterion — this is a fact about the
        # STAND, not a verdict about the binaries.
        print(stand_mutated_line(RESTORE_FAILED[-1]))
        return 1
    return rc


def _ungraded(*legs: dict) -> str:
    """The reason a criterion built from `legs` grades NOTHING, or "".

    A leg killed at `LEG_TIMEOUT` reports a wall-clock, a shed count and a row
    set that are all FLOORS of what the run would have produced. Comparing
    against a floor is not a measurement, and every one of these criteria is
    directional, so the floor lands on the PASSING side: `new <= old*1.05` when
    the old leg is the corpse, `new sheds 0` when the new one is.
    """
    dead = [r["tag"] for r in legs if r.get("timed_out")]
    if not dead:
        return ""
    return (f"UNGRADED: leg{'s' if len(dead) > 1 else ''} {', '.join(dead)} TIMED OUT at "
            f"{LEG_TIMEOUT:.0f}s - a killed leg's numbers are a floor, not a measurement")


def report(runs: dict) -> int:
    print(f"\n{'run':12}{'exit':>6}{'wall_s':>9}{'backed_off':>12}{'recovered':>11}{'exports':>9}")
    for side, adaptive in LEG_PLAN:
        r = runs[leg_tag(side, adaptive)]
        print(f"{r['tag']:12}{r['exit']:>6}{r['wall']:>9.1f}{r['backed_off']:>12}"
              f"{r['recovered']:>11}{len(r['rows']):>9}"
              + ("   ! TIMED OUT — a floor, not a measurement" if r["timed_out"] else ""))

    old_on, new_on = runs["old-on"], runs["new-on"]
    verdicts = []
    # Criterion 1 is the only one a timeout cannot flatter: it asks for a
    # POSITIVE observation (the old binary shed at least once), and a truncated
    # leg can only under-report one. So a timed-out old leg that still shed is
    # honest evidence the fixture is live — noted, not poisoned.
    old_shed = old_on["backed_off"]
    if not old_on["timed_out"]:
        note = ""
    elif old_shed > 0:
        # The note asserts an OBSERVATION, so it is gated on the observation and
        # not on the timeout alone. Gated on `timed_out` by itself it printed
        # `old shed 0x [leg timed out; a shed was observed before it did]` — one
        # detail string saying the fixture is dead and that it fired, and the
        # release-oracle copies this string verbatim into the row it labels
        # "CRITERION 1 — THE FIXTURE IS DEAD".
        note = " [leg timed out; a shed was observed before it did]"
    else:
        note = (" [leg timed out having observed NO shed — a truncated leg can only "
                "UNDER-report, so this is not evidence the symptom is absent]")
    verdicts.append(("1 fixture is live (old must shed)", old_shed > 0,
                     f"old shed {old_shed}x" + note))
    why = _ungraded(new_on)
    verdicts.append(("2 symptom gone (new sheds 0 on an idle source)",
                     not why and new_on["backed_off"] == 0,
                     why or f"new shed {new_on['backed_off']}x"))
    why = _ungraded(old_on, new_on)
    verdicts.append(("3 no makespan regression (new <= old * 1.05)",
                     not why and new_on["wall"] <= old_on["wall"] * 1.05,
                     why or f"{new_on['wall']:.1f}s vs {old_on['wall']:.1f}s"))
    why = _ungraded(old_on, new_on)
    same = old_on["rows"] == new_on["rows"] and bool(new_on["rows"])
    verdicts.append(("4 same data delivered", not why and same,
                     why or f"{len(new_on['rows'])} exports, "
                            f"{sum(new_on['rows'].values()):,} rows"))
    # Informational, not a gate: the adaptive-vs-baseline cost on each binary.
    for tag in ("old", "new"):
        on_leg, off_leg = runs[f"{tag}-on"], runs[f"{tag}-off"]
        on, off = on_leg["wall"], off_leg["wall"]
        floor = _ungraded(on_leg, off_leg)
        print(f"\n{tag}: adaptive ON {on:.1f}s vs OFF {off:.1f}s -> ratio {on / off:.2f}"
              + (f"  [{floor}]" if floor else ""))

    print()
    ok = True
    for name, passed, detail in verdicts:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})")
        ok &= passed
    # A non-zero exit on any leg does not change a criterion, but a reader must
    # not have to notice it in the table: the criteria above are only as good as
    # the legs that produced them.
    for tag, r in runs.items():
        if r["exit"] != 0:
            print(f"  ! leg {tag} exited {r['exit']}: {r['stderr_tail']}")
    return 0 if ok else 1


if __name__ == "__main__":
    shell_main(main)
