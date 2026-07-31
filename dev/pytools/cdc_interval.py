"""CDC drain-interval and drain-memory harnesses — one module, four subcommands.

Port of the whole `dev/cdc_interval/` bash family, which shares idioms (a
timestamped `say`, a file-flagged background loader, a `/usr/bin/time -l` drain,
a duckdb merge oracle) and is therefore ported as ONE module:

| bash                | subcommand      | question it answers |
|---------------------|-----------------|---------------------|
| `run.sh`            | `run`           | MySQL: does draining LESS often degrade anything? |
| `run_pg.sh`         | `run-pg`        | PostgreSQL: same, plus what the slot's WAL pin costs the SOURCE |
| `baseline_mem.sh`   | `baseline-mem`  | peak drain RSS vs backlog, per engine, one big drain |
| `soak_all.sh`       | `soak-all`      | one engine, growing intervals: does RSS stay FLAT and is every row captured? |

WHY THE TWO INTERVAL HARNESSES ARE DIFFERENT EXPERIMENTS, NOT A COPY: MySQL's
binlog is retained for replication/PITR whether or not rivet reads it, so a long
drain interval is *free* — bounded only by binlog retention. PostgreSQL's logical
slot PINS WAL: everything since the last slot advance is held on the SOURCE only
because rivet has not consumed it, so a long interval shows up as source disk
pressure. `run-pg` therefore measures `pg_current_wal_lsn() - restart_lsn` at the
end-of-interval peak (before the drain) and again after it (released), and runs a
continuous WAL sampler with a free-space watch beside the loader.

Preserved exactly: the thresholds (soak's 1.5× RSS-trend gate), the measurements
(peak RSS in MB, wall seconds, event counts), the verdict wording (`ok`,
`COUNT(a!=b)`, ` XOR`, ` CONS(a!=b)`, `FAIL`, `WARN(d2=…)`, `RSS-TREND-FAIL`),
the `RESULT`/`VERIFY`/`TOTAL` line shapes, and the env-var interface
(`RIVET_BIN`, `WORK`, `PHASES`, `N`, `BATCH`, `TICK`, `SKIP_SETUP`,
`MYSQL_CDC_URL`, `MYSQL_CONTAINER`, `PG_CDC_URL`, `PG_CONTAINER`, `SLOT`). Every
env var also has a flag, which the bash lacked.

DELIBERATE CHANGES, all in the loud-instead-of-silent direction:

* **`/usr/bin/time` flavour is detected.** The bash hard-codes the BSD/macOS form
  (`-l`, RSS in BYTES, value-first line). GNU `time` REJECTS `-l`, so on Linux
  `/usr/bin/time -l rivet run …` never ran rivet at all: the drain recorded
  `rows=?`, `rss=0MB` and the harness carried on. Worse, `rss=0` makes soak's
  trend gate `last > first*3/2` read `0 > 0` — a vacuous PASS on the one gate the
  soak exists for. We probe once, use `-l`/bytes or `-v`/kbytes accordingly, and
  a missing peak-RSS line is a `Fail`, never a reported 0.
* **The wall clock is a monotonic-epoch delta, exactly like the bash's two
  `date +%s` reads — we never scrape `time`'s own elapsed field.** That field is
  `0:01:23.45` on GNU, and this repo has already been bitten by
  `grep -oE '[0-9]+\\.[0-9]+'` over it yielding `23.45`, i.e. a 90 s run measured
  as 30 s. If a future change does read it, sum the colon-separated groups
  (`h*3600 + m*60 + s`); never take the last float.
* **The event count is parsed from the drain's CONTENT, not from a grep over two
  file paths.** `baseline_mem.sh`/`soak_all.sh` ran
  `grep -E "…rows:" "$WORK/d.out" "$WORK/d.err" | head -1 | tr -dc '0-9'`; with
  two file operands grep prefixes each hit with the FILENAME, and `tr -dc '0-9'`
  then keeps the path's digits too — any `WORK` containing a digit silently
  prefixed garbage onto the measured event count. (`run.sh`/`run_pg.sh` used
  `cat … | grep`, which is safe; the two harnesses that report the number as a
  RESULT are the two that had the bug.)
* **Missing tool / down container / absent slot is a `Fail` with a fix hint.**
  The bash sent every client's stderr to `/dev/null` and probed nothing, so a
  stopped container produced empty strings that flowed into the verdicts as an
  unexplained `COUNT(123!=)`. `soak_all.sh`'s per-engine `case` statements have
  no default arm either, so an unknown engine ran a full multi-hour soak that
  created nothing, inserted nothing and "measured" it.
* **Teardown runs on failure (`try/finally`).** The bash dropped its fixtures
  only on the happy path, so a mid-run failure left tables, an enabled CDC
  capture instance and — worst — a PostgreSQL replication slot behind, pinning
  WAL on the source forever and poisoning the next scenario. On Ctrl-C we
  deliberately KEEP everything and print how to clean up: an interrupted run is
  the documented post-kill reconcile fixture (see `dev/cdc_interval/README.md`),
  and dropping the slot there would break `SKIP_SETUP=1` resume by re-anchoring
  the next run past the undrained tail.
* **A failed drain is announced.** The bash ignored rivet's exit status
  entirely; a crashed drain looked like an empty one.

NOT changed, on purpose: a `FAIL` verdict still exits 0, because the bash did and
these logs are read by eye. Pass `--strict` to get exit 1 on any FAIL/WARN
verdict — opt-in, so the default transcript and exit code stay identical.

Usage (env-only invocation works exactly as before):

    RIVET_BIN=target/release/rivet python dev/pytools/cdc_interval.py run
    PHASES="10 2;20 2" python dev/pytools/cdc_interval.py run --skip-setup
    python dev/pytools/cdc_interval.py run-pg --phases "2 2;4 1;6 1"
    N=1000000 python dev/pytools/cdc_interval.py baseline-mem all
    python dev/pytools/cdc_interval.py soak-all pg --phases "10 20 30 60 120"
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/cdc_interval.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

# ── the ten-table heat spectrum ────────────────────────────────────────────────
# The point of the spectrum is that ONE stream with ONE checkpoint has to serve
# tables whose event rates differ by four orders of magnitude:
#   hot    2 tables — 250 inserts/tick + a tail UPDATE + a periodic purge-DELETE
#   warm   3 tables — 50 inserts/tick
#   cold   3 tables — 3 inserts every 6th tick (~30 s)
#   frozen 2 tables — 1 insert every 60th tick (~5 min), so a short interval
#                     legitimately sees ZERO events for them — a table with no
#                     events must still converge, which is where a "flush only
#                     what moved" bug would hide.
HOT = ("ci_h1", "ci_h2")
WARM = ("ci_w1", "ci_w2", "ci_w3")
COLD = ("ci_c1", "ci_c2", "ci_c3")
FROZEN = ("ci_f1", "ci_f2")
ALL_TABLES = HOT + WARM + COLD + FROZEN

LOADER_TICK_SECONDS = 5
HOT_BATCH, WARM_BATCH, COLD_BATCH, FROZEN_BATCH = 250, 50, 3, 1
COLD_EVERY, FROZEN_EVERY, PURGE_EVERY = 6, 60, 12
# Purge keeps ~60k rows of history per hot table (≈20 min at 250 rows/tick) and
# bounds one statement at 80k rows so a single DELETE cannot become the run's
# largest transaction — the memory question is memory-vs-BACKLOG.
PURGE_KEEP_ROWS, PURGE_LIMIT = 60000, 80000
UPDATE_MODULUS, UPDATE_TAIL_ROWS = 17, 5000
SEED_ROWS = 2000  # pre-seed, so the initial snapshots are non-trivial
QUIESCE_SECONDS = 7  # after touching PAUSE: let the in-flight batch land

# Hard-coded in the bash for baseline-mem/soak-all (only run/run-pg took URLs
# from the environment); kept hard-coded so a transcript compares.
PG_URL = "postgresql://rivet:rivet@127.0.0.1:5434/rivet"
MYSQL_URL = "mysql://rivet:rivet@127.0.0.1:3307/rivet"
MSSQL_URL = "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet"
MONGO_URL = "mongodb://127.0.0.1:27018/soakdb?directConnection=true&serverSelectionTimeoutMS=5000"
MSSQL_PASSWORD = "Rivet_Passw0rd!"

PG_CONTAINER = "rivet-postgres-cdc-1"
MYSQL_CONTAINER = "rivet-mysql-cdc-1"
MSSQL_CONTAINER = "rivet-mssql-cdc-1"
MONGO_CONTAINER = "rivet-mongo-rs-1"

COMPOSE_HINT = {
    "postgres": "docker compose --profile cdc up -d postgres-cdc",
    "mysql": "docker compose --profile cdc up -d mysql-cdc",
    "mssql": "docker compose --profile cdc up -d mssql-cdc",
    "mongo": "docker compose --profile cdc up -d mongo-rs",
}

ROLLOVER = 50000  # deliberately small: stresses the part machinery


# ── transcript ─────────────────────────────────────────────────────────────────
class Log:
    """The bash's `say()`: `[HH:MM:SS] [prefix] msg`, on STDOUT, tee'd to a file.

    Stdout on purpose. The documented invocation is
    `soak_all.sh pg > pg.soak.log 2>&1` and the results are grepped out of that
    file, so moving the transcript to stderr (shell.py's default for progress)
    would break the consumer. Diagnostics the bash never printed go to stderr via
    `shell.warn`/`shell.bad`, which keeps the transcript comparable line for line.
    """

    def __init__(self, path: Path | None = None, prefix: str = "") -> None:
        self.path = path
        self.prefix = prefix
        self._lock = threading.Lock()  # the loader/churner thread also says things

    def say(self, msg: str) -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {self.prefix}{msg}"
        with self._lock:
            print(line, flush=True)
            if self.path is not None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a") as fh:
                    fh.write(line + "\n")

    def append_raw(self, text: str) -> None:
        """duckdb's `2>>"$LOG"` — a failed oracle query leaves its error in the
        progress log rather than vanishing."""
        if not text or self.path is None:
            return
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a") as fh:
                fh.write(text if text.endswith("\n") else text + "\n")


# ── peak-RSS measurement ───────────────────────────────────────────────────────
# BSD/macOS `/usr/bin/time -l` writes "  <bytes>  maximum resident set size";
# GNU `/usr/bin/time -v` writes "\tMaximum resident set size (kbytes): <n>".
# The bash matched only the lowercase BSD spelling and divided by 1048576, i.e.
# it assumed bytes — so even if GNU had accepted `-l`, a kbyte value would have
# been reported ~1024× too small (a 300 MB peak as 0 MB).
_RSS_BSD = re.compile(r"^\s*(\d+)\s+maximum resident set size", re.MULTILINE)
_RSS_GNU = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")
_TIME_BIN = "/usr/bin/time"


@dataclass(frozen=True)
class TimeTool:
    argv: tuple[str, ...]
    pattern: re.Pattern[str]
    unit_bytes: int
    flavour: str

    def peak_rss_mb(self, stderr: str) -> int | None:
        m = self.pattern.search(stderr)
        if m is None:
            return None
        return int(m.group(1)) * self.unit_bytes // 1048576


def detect_time_tool() -> TimeTool:
    """Probe `/usr/bin/time` once, loudly.

    An unusable `time` is a broken harness, not a slow one: every RSS number and
    (via the trend gate) every soak verdict depends on it.
    """
    shell.require(_TIME_BIN, hint="install BSD or GNU time; the drain's peak RSS comes from it")
    probe = shell.run([_TIME_BIN, "-l", "true"], timeout=60)
    if probe.ok and _RSS_BSD.search(probe.out):
        return TimeTool((_TIME_BIN, "-l"), _RSS_BSD, 1, "bsd")
    probe = shell.run([_TIME_BIN, "-v", "true"], timeout=60)
    if probe.ok and _RSS_GNU.search(probe.out):
        return TimeTool((_TIME_BIN, "-v"), _RSS_GNU, 1024, "gnu")
    raise shell.Fail(
        f"{_TIME_BIN} reports no peak RSS with either -l (BSD) or -v (GNU)",
        hint="every RSS measurement and the soak's 1.5x trend gate depend on it; "
        "refusing to run and report 0 MB",
    )


# ── drain ──────────────────────────────────────────────────────────────────────
_ROWS_LINE = re.compile(r"^[ \t\r\f\v]+rows:")


def parse_rows(text: str) -> str:
    """The bash's `grep -E "^[[:space:]]+rows:" | head -1 | tr -dc '0-9'`.

    FIRST indented `rows:` line wins (a multi-export run reports several), then
    every non-digit is dropped, which also removes rivet's thousands separators.
    Returns `"?"` for "no such line", matching `${rows:-?}` — a `?` in a
    transcript is the tell that the drain produced no summary at all.
    """
    for line in text.splitlines():
        if _ROWS_LINE.match(line):
            digits = "".join(c for c in line if c in "0123456789")
            return digits or "?"
    return "?"


@dataclass
class Drain:
    rows: str
    seconds: int
    rss_mb: int

    def as_field(self) -> str:
        """`run.sh`/`run_pg.sh`'s drain() return value: "rows Ns NMB"."""
        return f"{self.rows} {self.seconds}s {self.rss_mb}MB"


def drain(rivet: Path, cfg: Path, work: Path, tt: TimeTool, log: Log) -> Drain:
    """One measured `rivet run` — events, wall seconds, peak RSS.

    Wall time is the difference of two whole-second epoch reads, exactly like the
    bash's `t0=$(date +%s)` / `t1=$(date +%s)`, so the truncation behaviour is
    unchanged. `time`'s own elapsed field is deliberately NOT parsed (see the
    module docstring: `0:01:23.45` scraped as `23.45` once turned a 90 s run into
    a 30 s one here).

    `timeout=None`: a 2 M-event backlog drain legitimately takes minutes, and a
    timeout kill would surface as returncode 124 and be misread as a measurement.
    """
    t0 = int(time.time())
    p = shell.run([*tt.argv, str(rivet), "run", "-c", str(cfg)], timeout=None)
    t1 = int(time.time())
    work.mkdir(parents=True, exist_ok=True)
    (work / "d.out").write_text(p.stdout)
    (work / "d.err").write_text(p.stderr)

    if not p.ok:
        # The bash ignored rivet's exit status entirely, so a crashed drain was
        # indistinguishable from an empty one (rows '?', and a d2 WARN at most).
        tail = (p.stderr or p.stdout).strip().splitlines()
        detail = tail[-1] if tail else "(no output)"
        shell.bad(f"drain exited {p.returncode} — the numbers below describe a FAILED run: {detail}")
        log.say(f"DRAIN FAILED (exit {p.returncode}): {detail}")
    rows = parse_rows(p.stdout + p.stderr)  # content, never a two-file grep
    if rows == "?":
        shell.warn(f"no '  rows:' line in the drain output ({work / 'd.err'})")

    rss = tt.peak_rss_mb(p.stderr)
    if rss is None:
        raise shell.Fail(
            f"no peak-RSS line in {work / 'd.err'} ({tt.flavour} /usr/bin/time)",
            hint="the bash reported rss=0MB here, which makes the 1.5x RSS-trend "
            "gate compare 0 > 0 and pass vacuously",
        )
    return Drain(rows, t1 - t0, rss)


# ── engine access ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Db:
    """One engine's client, as the bash's `PG()`/`M()`/`MS()`/`MO()` wrappers.

    `argv` ends with the statement-carrying flag, so the statement is the last
    argv element and never re-parsed by a shell — the bash interpolated table
    names, paths and a password containing `!` into command strings.
    """

    name: str
    container: str
    argv: tuple[str, ...]
    numeric_prefix: str = ""  # MSSQL's `SET NOCOUNT ON; `

    def sql(self, statement: str, *, timeout: float | None = 600) -> shell.Proc:
        return shell.docker_exec(self.container, *self.argv, statement, timeout=timeout)

    def q(self, statement: str, *, timeout: float | None = 600) -> str:
        """stdout, stripped — the `$(PG …)` form. stderr is dropped like the
        bash's `2>/dev/null`; callers that care use `sql()` for the status."""
        return self.sql(statement, timeout=timeout).stdout.strip()

    def first_int(self, statement: str, *, timeout: float | None = 600) -> int | None:
        """The bash's `MSN()`: `SET NOCOUNT ON` kills the "(N rows affected)"
        trailer and we take the FIRST integer token, so a COUNT never fuses with
        a rowcount line. `None` means "no integer at all" — the bash's `${x:-0}`
        turned that into a real-looking zero."""
        out = self.q(self.numeric_prefix + statement, timeout=timeout)
        m = re.search(r"\d+", out)
        return int(m.group()) if m else None


def pg_db(container: str = PG_CONTAINER) -> Db:
    return Db("postgres", container, ("psql", "-U", "rivet", "-d", "rivet", "-tAc"))


def mysql_db(container: str = MYSQL_CONTAINER) -> Db:
    return Db("mysql", container, ("mysql", "-urivet", "-privet", "rivet", "-N", "-e"))


def mssql_db(container: str = MSSQL_CONTAINER) -> Db:
    return Db(
        "mssql",
        container,
        (
            "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost",
            "-U", "sa", "-P", MSSQL_PASSWORD, "-d", "rivet", "-h", "-1", "-Q",
        ),
        numeric_prefix="SET NOCOUNT ON; ",
    )


def mongo_db(container: str = MONGO_CONTAINER, dbname: str = "soakdb") -> Db:
    # The whole db is watched; `sk` is the collection and `_id` the key.
    return Db("mongo", container, ("mongosh", "--quiet", "--port", "27017", dbname, "--eval"))


_PROBE = {
    "postgres": "SELECT 1",
    "mysql": "SELECT 1",
    "mssql": "SELECT 1",
    "mongo": "print(db.runCommand({ping:1}).ok)",
}


def require_db(db: Db) -> None:
    """Container running AND answering — probed, not assumed.

    The bash probed nothing and discarded every client's stderr, so a stopped
    container yielded empty strings that reached the verdicts as an unexplained
    `COUNT(123!=)` hours later. This is the same shape as the `case` with no
    default arm that once reported a store UP without contacting it.
    """
    hint = COMPOSE_HINT[db.name]
    p = shell.docker("inspect", "-f", "{{.State.Running}}", db.container, timeout=60)
    if not p.ok or p.stdout.strip() != "true":
        raise shell.Fail(f"container {db.container} is not running", hint=hint)
    if db.first_int(_PROBE[db.name]) is None:
        raise shell.Fail(
            f"{db.name} in {db.container} did not answer a probe query", hint=hint
        )


# ── binary + oracle preconditions ──────────────────────────────────────────────
def resolve_rivet(explicit: str | None, *, debug_fallback: bool) -> Path:
    """`RIVET_BIN`, else `target/release/rivet` — CWD-relative, as the bash was.

    `baseline_mem.sh`/`soak_all.sh` add `[ -x "$RIVET_BIN" ] || RIVET_BIN=target/
    debug/rivet`; `run.sh`/`run_pg.sh` do not, and with a missing binary they
    died inside `/usr/bin/time` and recorded `rows=?` for every drain instead of
    saying so. All four now refuse up front.
    """
    candidate = Path(explicit or os.environ.get("RIVET_BIN") or "target/release/rivet")
    if debug_fallback and not (candidate.is_file() and os.access(candidate, os.X_OK)):
        candidate = Path("target/debug/rivet")
    if not (candidate.is_file() and os.access(candidate, os.X_OK)):
        raise shell.Fail(
            f"rivet binary {candidate} is not executable (cwd {Path.cwd()})",
            hint="cargo build --release --bin rivet, or set RIVET_BIN=/path/to/rivet",
        )
    return candidate


def require_oracle() -> None:
    shell.require(
        "duckdb",
        hint="brew install duckdb — it is the destination-side merge oracle; "
        "without it every convergence verdict would be an empty-string mismatch",
    )


def duckdb_csv(sql: str, *, log: Log | None) -> str:
    """One `duckdb -noheader -csv -c <sql>`, returning the trimmed stdout.

    A failed query returns "" — which the bash then compared against a source
    count and reported as a data mismatch. Same value, but now the duckdb error
    is announced instead of only appended to the progress log.
    """
    p = shell.run(["duckdb", "-noheader", "-csv", "-c", sql], timeout=None)
    if log is not None:
        log.append_raw(p.stderr)
    if not p.ok:
        tail = p.stderr.strip().splitlines()
        shell.warn("duckdb oracle query failed: " + (tail[-1] if tail else "(no stderr)"))
    return p.stdout.strip()


# ── background workers ─────────────────────────────────────────────────────────
class Flag:
    """A sentinel FILE, as in the bash (`$WORK/loader.stop`, `loader.pause`).

    Kept file-backed rather than becoming a pure `threading.Event`, because the
    bash contract lets an operator pause or stop a detached multi-hour run with a
    `touch`, and that is genuinely used.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    @property
    def is_set(self) -> bool:
        return self.path.exists()

    def set(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch()

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


class Stop:
    """Stop = the sentinel file (operator-visible) OR an in-process Event.

    The Event exists so a `finally` can stop the writer promptly and reliably: a
    crash in the main thread must not leave a loader inserting into a table that
    is about to be dropped.
    """

    def __init__(self, path: Path) -> None:
        self.flag = Flag(path)
        self.event = threading.Event()

    @property
    def stopped(self) -> bool:
        return self.event.is_set() or self.flag.is_set

    def set(self) -> None:
        self.event.set()
        self.flag.set()

    def clear(self) -> None:
        self.event.clear()
        self.flag.clear()

    def wait(self, seconds: float) -> bool:
        """Sleep up to `seconds`, polling the sentinel file each second.
        Returns True if we should stop."""
        deadline = time.monotonic() + seconds
        while True:
            if self.stopped:
                return True
            left = deadline - time.monotonic()
            if left <= 0:
                return self.stopped
            self.event.wait(min(left, 1.0))


def spawn(fn: Callable[[], None], name: str) -> threading.Thread:
    t = threading.Thread(target=fn, name=name, daemon=True)
    t.start()
    return t


def phases_pairs(spec: str) -> list[tuple[int, int]]:
    """`"10 2;20 2;30 2"` -> [(10, 2), (20, 2), (30, 2)].

    The bash did `IFS=';' … set -- $phase; mins=$1; reps=$2`, which under `set -u`
    aborted mid-run on a one-field phase (and left `IFS` clobbered afterwards).
    An empty schedule is refused rather than producing a vacuous all-green run.
    """
    out: list[tuple[int, int]] = []
    for chunk in spec.split(";"):
        fields = chunk.split()
        if not fields:
            continue
        if len(fields) != 2:
            raise shell.Fail(f"phase {chunk.strip()!r} is not '<minutes> <reps>'")
        try:
            out.append((int(fields[0]), int(fields[1])))
        except ValueError as e:
            raise shell.Fail(f"phase {chunk.strip()!r} is not numeric: {e}") from e
    if not out:
        raise shell.Fail("PHASES is empty — nothing would be measured")
    return out


def phases_minutes(spec: str) -> list[int]:
    """soak's schedule: space-separated minutes, `"10 20 30 60 120"`."""
    try:
        out = [int(x) for x in spec.split()]
    except ValueError as e:
        raise shell.Fail(f"PHASES must be space-separated minutes: {e}") from e
    if not out:
        raise shell.Fail("PHASES is empty — nothing would be measured")
    return out


def pad(prefix: str) -> str:
    """`p_` / `s_` + 38 x's: a ~40-char payload so events are not trivially tiny
    (the bash built it with `printf '%*s' 38 '' | tr ' ' x`)."""
    return prefix + "x" * 38


# ==== INTERVAL HARNESSES (run.sh / run_pg.sh) ==================================
def split_pair(text: str) -> tuple[str, str]:
    """The bash's `dc=${mm% *}; dx=${mm#* }` — before the LAST space, after the
    FIRST. On duckdb's single "count xor" field those are the two values; on ""
    (a failed oracle query) BOTH are "", so every comparison mismatches and the
    verdict is FAIL. Same behaviour, and the right direction for an integrity
    gate — an unavailable oracle must never read as convergence."""
    if " " not in text:
        return text, text  # bash: neither expansion matches, both yield $mm
    return text.rsplit(" ", 1)[0], text.split(" ", 1)[1]


class IntervalRun:
    """Shared skeleton of `run.sh` (MySQL) and `run_pg.sh` (PostgreSQL).

    Identical between the two: the ten-table spectrum, the loader cadence, the
    per-table id cursors in `$WORK/next.<table>`, the drain-measure-drain
    protocol, the conservation query, and every verdict string. The subclasses
    supply only what the engines genuinely disagree on — DDL, string-literal
    quoting, the bounded DELETE (MySQL has `DELETE … LIMIT`, PostgreSQL needs a
    `ctid IN (…)` subselect), the source-side XOR, the destination merge's
    ordering key, and PostgreSQL's slot/WAL instrumentation.
    """

    engine = ""
    quote = "'"

    def __init__(
        self,
        *,
        rivet: Path,
        work: Path,
        db: Db,
        phases: str,
        skip_setup: bool,
        tt: TimeTool,
        strict: bool,
    ) -> None:
        self.rivet = rivet
        self.work = work
        self.out = work / "out"
        self.cfg = work / "cdc.yaml"
        self.ckpt = work / "cdc.ckpt"
        self.log = Log(work / "progress.log")
        self.db = db
        self.phases = phases_pairs(phases)
        self.skip_setup = skip_setup
        self.tt = tt
        self.strict = strict
        self.stop = Stop(work / "loader.stop")
        self.pause = Flag(work / "loader.pause")
        self.failures = 0
        self._workers: list[threading.Thread] = []

    # ── engine hooks ───────────────────────────────────────────────────────────
    def cfg_text(self) -> str:
        raise NotImplementedError

    def create_sql(self, t: str) -> str:
        raise NotImplementedError

    def purge_sql(self, t: str, cut: int) -> str:
        raise NotImplementedError

    def source_xor_sql(self, t: str) -> str:
        raise NotImplementedError

    def merge_sql(self, t: str, snap: bool, cdc: bool) -> str:
        raise NotImplementedError

    def pin_banner(self) -> str:
        return "=== pin + initial snapshots ==="

    def pin_ok_message(self) -> str:
        return "pin+snapshots ok"

    def phase_banner(self, mins: int, rep: int, reps: int) -> str:
        return f"--- phase {mins}min rep {rep}/{reps} ---"

    def complete_banner(self) -> str:
        return "=== complete ==="

    def setup_extra(self) -> None:
        """Engine-specific fresh-start work, before the tables are created."""

    def verify_prologue(self) -> None:
        """PostgreSQL samples the end-of-interval WAL peak here."""

    def verify_middle(self) -> str:
        """Segment inserted into the VERIFY line after `d2=[…]`."""
        return ""

    def teardown_extra(self, *, keep: bool) -> None:
        """PostgreSQL releases (or reports) the replication slot."""

    def start_extra_workers(self) -> None:
        """PostgreSQL starts the WAL sampler beside the loader."""

    # ── shared SQL ─────────────────────────────────────────────────────────────
    def table_list(self) -> str:
        return ", ".join(ALL_TABLES)

    def seed_sql(self, t: str) -> str:
        q = self.quote
        vals = ",".join(
            f"({i},{i * 3},{q}seed{i % 97}{q})" for i in range(1, SEED_ROWS + 1)
        )
        return f"INSERT INTO {t} (id,v,pad) VALUES {vals}"

    def insert_sql(self, t: str, a: int, b: int) -> str:
        q = self.quote
        vals = ",".join(f"({i},{i * 7},{q}p{i % 1000}{q})" for i in range(a, b + 1))
        return f"INSERT INTO {t} (id,v,pad) VALUES {vals}"

    def update_sql(self, t: str, tick: int, floor_id: int) -> str:
        # Rewrites ~1/17th of the last 5000 rows, so the hot tables emit UPDATE
        # events (not just inserts) and the row_hash XOR can actually diverge.
        return (
            f"UPDATE {t} SET v=v+1 WHERE id % {UPDATE_MODULUS} = {tick % UPDATE_MODULUS} "
            f"AND id > {floor_id}"
        )

    def source_count_sql(self, t: str) -> str:
        return f"SELECT COUNT(*) FROM {t}"

    def seen_sql(self, t: str, cdc: bool) -> str:
        """Conservation: every id EVER inserted must appear as an insert event or
        in the snapshot — including rows the purge later deleted."""
        arm = (
            f"SELECT id FROM read_parquet('{self.out}/{t}/cdc-*.parquet') "
            "WHERE __op='insert' UNION"
            if cdc
            else ""
        )
        return f"""
      SELECT COUNT(DISTINCT id) FROM (
        {arm}
        SELECT id FROM read_parquet('{self.out}/{t}/snapshot/*.parquet'))"""

    # ── per-table id cursors ───────────────────────────────────────────────────
    def next_path(self, t: str) -> Path:
        return self.work / f"next.{t}"

    def read_next(self, t: str) -> int:
        p = self.next_path(t)
        try:
            text = p.read_text().strip()
        except FileNotFoundError:
            raise shell.Fail(
                f"{p} is missing — the per-table id cursor state is gone",
                hint="drop --skip-setup to recreate the tables and cursors",
            ) from None
        if not text.isdigit():
            raise shell.Fail(f"{p} does not hold a number: {text!r}")
        return int(text)

    def write_next(self, t: str, value: int) -> None:
        shell.atomic_write(self.next_path(t), f"{value}\n")

    # ── phases ─────────────────────────────────────────────────────────────────
    def setup(self) -> None:
        self.setup_extra()
        for t in ALL_TABLES:
            self.db.sql(self.create_sql(t), timeout=None).check(f"create {t}")
            self.write_next(t, 1)
        for t in ALL_TABLES:  # pre-seed so the initial snapshots are non-trivial
            self.db.sql(self.seed_sql(t), timeout=None).check(f"seed {t}")
            self.write_next(t, SEED_ROWS + 1)
        self.log.say(f"pre-seeded {len(ALL_TABLES)} tables x {SEED_ROWS} rows")

    def pin(self) -> bool:
        self.log.say(self.pin_banner())
        p = shell.run([str(self.rivet), "run", "-c", str(self.cfg)], timeout=None)
        (self.work / "d.out").write_text(p.out)  # bash: > d.out 2>&1
        if p.ok:
            self.log.say(self.pin_ok_message())
            return True
        tail = "\n".join(p.out.strip().splitlines()[-3:])
        self.log.say(f"PIN FAILED: {tail}")
        return False

    def loader(self) -> None:
        """~250 change events/s across the spectrum (2x250 + 3x50 inserts per 5 s
        tick, plus the hot tables' tail UPDATEs), with a periodic purge-DELETE.

        Wrapped so a thread death is LOUD: a silently dead loader would leave
        every later interval with no load at all, and the harness would then
        report perfect convergence on an idle database.
        """
        try:
            self._loader_body()
        except BaseException as e:  # noqa: BLE001 - a dead writer must be announced
            shell.bad(f"loader thread died ({e!r}) — later intervals carry NO load")
            self.log.say(f"loader ABORTED: {e!r}")

    def _loader_body(self) -> None:
        tick = 0
        while not self.stop.stopped:
            if self.pause.is_set:
                if self.stop.wait(1):
                    break
                continue
            tick += 1
            for t in HOT:
                self.insert_batch(t, HOT_BATCH)
                # Reads the cursor AFTER the insert, as the bash did: the update
                # window is the newest 5000 ids.
                self.db.sql(
                    self.update_sql(t, tick, self.read_next(t) - UPDATE_TAIL_ROWS),
                    timeout=None,
                )
            for t in WARM:
                self.insert_batch(t, WARM_BATCH)
            if tick % COLD_EVERY == 0:
                for t in COLD:
                    self.insert_batch(t, COLD_BATCH)
            if tick % FROZEN_EVERY == 0:
                for t in FROZEN:
                    self.insert_batch(t, FROZEN_BATCH)
            if tick % PURGE_EVERY == 0:
                for t in HOT:
                    cut = self.read_next(t) - PURGE_KEEP_ROWS
                    if cut > SEED_ROWS:  # never purge into the seeded prefix
                        self.db.sql(self.purge_sql(t, cut), timeout=None)
            self.stop.wait(LOADER_TICK_SECONDS)
        self.log.say("loader stopped")

    def insert_batch(self, t: str, n: int) -> None:
        a = self.read_next(t)
        b = a + n - 1
        if not self.db.sql(self.insert_sql(t, a, b), timeout=None).ok:
            self.log.say(f"loader: {t} insert @{a} failed")
        # The cursor advances even on failure, exactly as in the bash: it records
        # ids ATTEMPTED, so a lost insert surfaces as a CONS(seen!=max) verdict
        # rather than being quietly forgiven.
        self.write_next(t, b + 1)

    def join_workers(self) -> None:
        for t in self._workers:
            t.join(timeout=LOADER_TICK_SECONDS + 10)
        self._workers = [t for t in self._workers if t.is_alive()]

    # ── the drain-measure-drain protocol ───────────────────────────────────────
    def verify(self) -> None:
        self.verify_prologue()
        self.pause.set()
        time.sleep(QUIESCE_SECONDS)  # let the in-flight batch land, then quiesce
        d1 = drain(self.rivet, self.cfg, self.work, self.tt, self.log)

        src_count: dict[str, str] = {}
        src_xor: dict[str, str] = {}
        src_max: dict[str, str] = {}
        for t in ALL_TABLES:
            src_count[t] = self.db.q(self.source_count_sql(t), timeout=None)
            src_xor[t] = self.db.q(self.source_xor_sql(t), timeout=None)
            src_max[t] = str(self.read_next(t) - 1)
            # The bash kept these in $WORK/{sc,sx,mx}.<table>; keep the artifacts
            # so a post-mortem can see what the source claimed at drain time.
            shell.atomic_write(self.work / f"sc.{t}", src_count[t] + "\n")
            shell.atomic_write(self.work / f"sx.{t}", src_xor[t] + "\n")
            shell.atomic_write(self.work / f"mx.{t}", src_max[t] + "\n")

        # Must report 0: nothing ran between the two drains, so a non-zero second
        # drain means the first one did not reach the source's current position.
        d2 = drain(self.rivet, self.cfg, self.work, self.tt, self.log)
        middle = self.verify_middle()
        self.pause.clear()

        verdict = "OK"
        line = ""
        for t in ALL_TABLES:
            snap = any((self.out / t / "snapshot").glob("*.parquet"))
            cdc = any((self.out / t).glob("cdc-*.parquet"))
            dc, dx = split_pair(duckdb_csv(self.merge_sql(t, snap, cdc), log=self.log))
            seen = duckdb_csv(self.seen_sql(t, cdc), log=self.log)
            v = "ok"
            if src_count[t] != dc:
                v = f"COUNT({src_count[t]}!={dc})"
            if src_xor[t] != dx:
                v = f"{v} XOR"
            if seen != src_max[t]:
                v = f"{v} CONS({seen}!={src_max[t]})"
            if v != "ok":
                verdict = "FAIL"
            line += f" {t}={v}"

        if d2.rows != "0":
            verdict = f"{verdict} WARN(d2={d2.rows})"
        self.log.say(
            f"VERIFY d1=[{d1.as_field()}] d2=[{d2.as_field()}]{middle} =>{line}"
        )
        parts = len(list(self.out.rglob("cdc-*.parquet")))
        self.log.say(f"TOTAL: {verdict} | parts={parts}")
        if verdict != "OK":
            self.failures += 1

    # ── driver ─────────────────────────────────────────────────────────────────
    def main(self) -> int:
        self.out.mkdir(parents=True, exist_ok=True)
        self.pause.clear()
        self.stop.clear()
        shell.atomic_write(self.cfg, self.cfg_text())

        if not self.skip_setup:
            self.setup()
            if not self.pin():
                # The bash exits 1 here WITHOUT dropping the tables — deliberate:
                # a failed pin is exactly what you want to inspect.
                return 1

        interrupted = False
        try:
            self._workers.append(spawn(self.loader, "loader"))
            self.log.say(f"loader pid={os.getpid()} (in-process thread)")
            self.start_extra_workers()
            for mins, reps in self.phases:
                for rep in range(1, reps + 1):
                    self.log.say(self.phase_banner(mins, rep, reps))
                    time.sleep(mins * 60)
                    self.verify()
            self.stop.set()
            self.join_workers()
            self.log.say("=== final verify after loader stop ===")
            self.verify()
        except KeyboardInterrupt:
            interrupted = True
            raise
        finally:
            self.stop.set()
            self.join_workers()
            self.pause.clear()
            self.teardown(keep=interrupted)

        self.log.say(self.complete_banner())
        return 1 if (self.strict and self.failures) else 0

    def teardown(self, *, keep: bool) -> None:
        """Drop the fixtures — on the happy path AND on failure.

        The bash dropped them only after the final verify, so any mid-run failure
        left ten loaded tables (and, on PostgreSQL, a WAL-pinning slot) behind for
        the next scenario to trip over.

        Ctrl-C is the one case that KEEPS everything: an interrupted run is the
        documented post-kill reconcile fixture (`dev/cdc_interval/README.md` — the
        kill that became a crash test), and `SKIP_SETUP=1` resumes from exactly
        this state.
        """
        if keep:
            shell.warn("interrupted — tables, checkpoint and parts left in place")
            self.log.say("=== interrupted: fixtures kept (resume with SKIP_SETUP=1) ===")
            self.teardown_extra(keep=True)
            return
        for t in ALL_TABLES:
            self.db.sql(f"DROP TABLE IF EXISTS {t}", timeout=None)
        self.teardown_extra(keep=False)


# ── MySQL (run.sh) ─────────────────────────────────────────────────────────────
class MysqlIntervalRun(IntervalRun):
    """MySQL edition. The binlog is retained for replication/PITR regardless of
    rivet, so stretching the drain interval costs the SOURCE nothing — the only
    ceiling is binlog retention. That is why there is no WAL-style instrument
    here: there is nothing accruing to measure."""

    engine = "mysql"
    quote = '"'  # MySQL accepts double-quoted string literals (no ANSI_QUOTES)

    def __init__(self, *, url: str, **kw) -> None:
        self.url = url
        super().__init__(**kw)

    def cfg_text(self) -> str:
        return f"""source: {{ type: mysql, url: "{self.url}" }}
exports:
  - name: cdc_interval
    tables: [{self.table_list()}]
    mode: cdc
    format: parquet
    cdc: {{ initial: snapshot, checkpoint: "{self.ckpt}", until_current: true, server_id: 48121, rollover: {ROLLOVER} }}
    destination: {{ type: local, path: "{self.out}" }}
"""

    def create_sql(self, t: str) -> str:
        # row_hash is a STORED generated column: the source-side oracle and the
        # destination-side one then hash the same bytes by construction, so the
        # XOR comparison cannot be a self-oracle over rivet's own rendering.
        return (
            f"DROP TABLE IF EXISTS {t}; "
            f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, "
            "pad VARCHAR(64) NOT NULL, "
            "row_hash CHAR(32) AS (MD5(CONCAT_WS('#',id,v,pad))) STORED);"
        )

    def purge_sql(self, t: str, cut: int) -> str:
        return (
            f"DELETE FROM {t} WHERE id < {cut} AND id > {SEED_ROWS} LIMIT {PURGE_LIMIT}"
        )

    def source_xor_sql(self, t: str) -> str:
        # 15 hex digits = 60 bits, which CONV()/BIT_XOR handle without overflow
        # and which duckdb's UBIGINT matches on the destination side.
        return f"SELECT COALESCE(BIT_XOR(CONV(SUBSTRING(row_hash,1,15),16,10)),0) FROM {t}"

    def merge_sql(self, t: str, snap: bool, cdc: bool) -> str:
        """Reconstruct the current state from snapshot ∪ events.

        Winner per id is the highest (binlog file, pos). The file name is
        compared as a string and the pos as an integer, parsed out of `__pos`
        JSON — the raw `__pos` string is NOT a valid order across a rotation.
        The snapshot arm carries f='' / p=-1 so any real event outranks it.
        """
        snap_arm = (
            f"""
    SELECT id,row_hash,'' AS f,-1::BIGINT AS p,'insert' AS op
    FROM read_parquet('{self.out}/{t}/snapshot/*.parquet') UNION ALL"""
            if snap
            else ""
        )
        if cdc:
            return f"""
      WITH uni AS ({snap_arm}
        SELECT id,row_hash, json_extract_string(__pos,'$.file') AS f,
               CAST(json_extract(__pos,'$.pos') AS BIGINT) AS p, __op AS op
        FROM read_parquet('{self.out}/{t}/cdc-*.parquet')),
      r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY f DESC,p DESC) rn FROM uni)
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM r WHERE rn=1 AND op<>'delete'"""
        return f"""
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM read_parquet('{self.out}/{t}/snapshot/*.parquet')"""

    def setup_extra(self) -> None:
        # BASH BUG, reported not silently fixed: run.sh recreates the tables (ids
        # restart at 1) but — unlike run_pg.sh — never removes $WORK/cdc.ckpt or
        # $WORK/out. A second non-SKIP_SETUP run therefore resumes from a stale
        # checkpoint (no fresh snapshots) and merges the previous run's parts into
        # every verdict. Warn loudly; deleting them here would change what the
        # harness measures.
        stale = [p for p in (self.ckpt,) if p.exists()]
        stale += list(self.out.rglob("cdc-*.parquet"))[:1]
        if stale:
            shell.warn(
                f"{self.ckpt} / previous parts under {self.out} still exist and "
                "run.sh never cleaned them: this run will resume from the stale "
                f"checkpoint and merge old parts into its verdicts (rm -rf {self.out} "
                f"{self.ckpt} for a true fresh start)"
            )


# ── PostgreSQL (run_pg.sh) ─────────────────────────────────────────────────────
class PostgresIntervalRun(IntervalRun):
    """PostgreSQL edition — the experiment that has something extra to measure.

    A logical slot PINS WAL: everything since the last advance is retained on the
    SOURCE only because rivet has not consumed it. So a longer drain interval is
    NOT free here; it accrues as source disk pressure, released only when the
    drain advances the slot. `retained WAL` = pg_current_wal_lsn() - restart_lsn,
    sampled at the end-of-interval peak (before the drain) and again after it.
    """

    engine = "postgres"
    quote = "'"

    def __init__(self, *, url: str, slot: str, **kw) -> None:
        self.url = url
        self.slot = slot
        self._wal_peak = 0
        super().__init__(**kw)
        self.wal_trace = self.work / "wal_trace.csv"

    def cfg_text(self) -> str:
        return f"""source: {{ type: postgres, url: "{self.url}" }}
exports:
  - name: cdc_interval
    tables: [{self.table_list()}]
    mode: cdc
    format: parquet
    cdc: {{ initial: snapshot, checkpoint: "{self.ckpt}", slot: {self.slot}, until_current: true, rollover: {ROLLOVER} }}
    destination: {{ type: local, path: "{self.out}" }}
"""

    def create_sql(self, t: str) -> str:
        return (
            f"DROP TABLE IF EXISTS {t};\n"
            f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, "
            "pad VARCHAR(64) NOT NULL,\n"
            "  row_hash TEXT GENERATED ALWAYS AS "
            "(md5(id::text||'#'||v::text||'#'||pad)) STORED);"
        )

    def purge_sql(self, t: str, cut: int) -> str:
        # PostgreSQL's DELETE has no LIMIT — bound it through a ctid subselect so
        # one purge cannot become the run's largest transaction.
        return (
            f"DELETE FROM {t} WHERE ctid IN "
            f"(SELECT ctid FROM {t} WHERE id < {cut} AND id > {SEED_ROWS} "
            f"LIMIT {PURGE_LIMIT})"
        )

    def source_xor_sql(self, t: str) -> str:
        return (
            "SELECT COALESCE(bit_xor(('x'||substring(row_hash,1,15))::bit(60)::bigint),0) "
            f"FROM {t}"
        )

    def merge_sql(self, t: str, snap: bool, cdc: bool) -> str:
        """As MySQL, but the ordering key is different in a way that matters.

        PostgreSQL's `__pos` is `{lsn:"X/Y"}` — the COMMIT LSN — and the literal
        "X/Y" string is NOT sortable across widths ("A/9" vs "10/1"). The order is
        the fixed-width hex key upper(lpad(hi,8,'0'))||upper(lpad(lo,8,'0')),
        tie-broken by (filename, file_row_number) because every event of one
        transaction shares the commit LSN.
        """
        snap_arm = (
            f"""
    SELECT id,row_hash,'' AS lsn_key,'insert' AS op,'' AS fn,-1::BIGINT AS frn
    FROM read_parquet('{self.out}/{t}/snapshot/*.parquet') UNION ALL"""
            if snap
            else ""
        )
        if cdc:
            return f"""
      WITH uni AS ({snap_arm}
        SELECT id,row_hash,
          upper(lpad(split_part(json_extract_string(__pos,'$.lsn'),'/',1),8,'0')) ||
          upper(lpad(split_part(json_extract_string(__pos,'$.lsn'),'/',2),8,'0')) AS lsn_key,
          __op AS op, filename AS fn, file_row_number AS frn
        FROM read_parquet('{self.out}/{t}/cdc-*.parquet', filename=true, file_row_number=true)),
      r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY lsn_key DESC, fn DESC, frn DESC) rn FROM uni)
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM r WHERE rn=1 AND op<>'delete'"""
        return f"""
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM read_parquet('{self.out}/{t}/snapshot/*.parquet')"""

    # ── slot / WAL instrumentation ─────────────────────────────────────────────
    def wal_retained_mb(self) -> int:
        raw = self.db.q(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint "
            f"FROM pg_replication_slots WHERE slot_name='{self.slot}'"
        )
        if not raw.strip().lstrip("-").isdigit():
            # The bash's `${b:-0}` printed a confident 0 MB for "slot absent" and
            # for "query failed" alike — the two states the whole experiment is
            # about. Say which one it is.
            shell.warn(
                f"retained-WAL query returned {raw!r} (slot {self.slot} missing, or "
                "the query failed) — reporting 0MB"
            )
            return 0
        return int(raw) // 1048576

    def slot_line(self) -> str:
        return self.db.q(
            "SELECT active::text||' '||wal_status||' restart='||restart_lsn"
            "||' confirmed='||confirmed_flush_lsn "
            f"FROM pg_replication_slots WHERE slot_name='{self.slot}'"
        )

    def drop_slot(self) -> None:
        self.db.q(
            f"SELECT pg_drop_replication_slot('{self.slot}') FROM pg_replication_slots "
            f"WHERE slot_name='{self.slot}'"
        )

    def setup_extra(self) -> None:
        # Start clean: dropping the slot frees the WAL it pinned, and the
        # checkpoint + parts must go with it or the next run resumes into them.
        self.drop_slot()
        shell.rm_rf(self.ckpt)
        shell.rm_rf(self.out)
        self.out.mkdir(parents=True, exist_ok=True)

    def pin_banner(self) -> str:
        return "=== pin slot + initial snapshots ==="

    def pin_ok_message(self) -> str:
        return f"pin+snapshots ok | {self.slot_line()}"

    def phase_banner(self, mins: int, rep: int, reps: int) -> str:
        return f"--- phase {mins}min rep {rep}/{reps} (slot pinning WAL for {mins}min) ---"

    def complete_banner(self) -> str:
        return "=== complete (slot dropped, WAL released) ==="

    def verify_prologue(self) -> None:
        self._wal_peak = self.wal_retained_mb()  # end-of-interval peak
        line = self.slot_line()
        if not line:
            shell.warn(
                f"replication slot {self.slot} does not exist — the drain below is "
                "not measuring a pinned-WAL interval at all"
            )
        self.log.say(f"  slot before drain: {line} | retained WAL={self._wal_peak}MB")

    def verify_middle(self) -> str:
        return f" WAL:{self._wal_peak}MB->{self.wal_retained_mb()}MB"

    def start_extra_workers(self) -> None:
        self._workers.append(spawn(self.wal_sampler, "wal_sampler"))
        self.log.say(
            f"wal_sampler pid={os.getpid()} (in-process thread; trace -> {self.wal_trace})"
        )

    def wal_sampler(self) -> None:
        """The disk-pressure curve, plus a free-space watch.

        Two columns because they answer different questions: `retained_mb` is what
        the SLOT is holding (rivet's fault, released by a drain) and `waldir_mb` is
        what is actually on disk (includes checkpoints/archives). `vol_avail_mb`
        is the safety line — a slot experiment that fills the data volume takes
        the container with it. `df -Pm` for the POSIX single-line format, so the
        4th field is always Available.
        """
        try:
            self.wal_trace.parent.mkdir(parents=True, exist_ok=True)
            self.wal_trace.write_text("epoch,retained_mb,waldir_mb,vol_avail_mb\n")
            while not self.stop.stopped:
                b = self.db.q(
                    "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)"
                    f"::bigint FROM pg_replication_slots WHERE slot_name='{self.slot}'"
                )
                w = self.db.q("SELECT COALESCE(sum(size),0)::bigint FROM pg_ls_waldir()")
                df = shell.docker_exec(
                    self.db.container, "df", "-Pm", "/var/lib/postgresql/data"
                )
                lines = df.stdout.splitlines()
                fields = lines[1].split() if len(lines) > 1 else []
                free = fields[3] if len(fields) > 3 else "?"
                retained = int(b) // 1048576 if b.lstrip("-").isdigit() else 0
                waldir = int(w) // 1048576 if w.lstrip("-").isdigit() else 0
                with self.wal_trace.open("a") as fh:
                    fh.write(f"{int(time.time())},{retained},{waldir},{free}\n")
                self.stop.wait(120)
        except BaseException as e:  # noqa: BLE001
            shell.bad(f"wal_sampler thread died ({e!r}) — the WAL trace stops here")

    def teardown_extra(self, *, keep: bool) -> None:
        if keep:
            # Do NOT drop the slot on Ctrl-C: the checkpoint still references it,
            # and re-creating it later would anchor at a NEWER position, silently
            # skipping the undrained tail. Leaving it pins WAL, so say so.
            shell.warn(
                f"slot {self.slot} is STILL PINNING WAL on {self.db.container}. "
                "Resume with SKIP_SETUP=1, or release it with: SELECT "
                f"pg_drop_replication_slot('{self.slot}');"
            )
            return
        self.drop_slot()


def run_mysql(
    *,
    rivet: str | None = None,
    work: str | None = None,
    url: str | None = None,
    container: str | None = None,
    phases: str | None = None,
    skip_setup: bool | None = None,
    strict: bool = False,
) -> int:
    """`dev/cdc_interval/run.sh`."""
    require_oracle()
    shell.require("docker", hint="the source runs in a compose container")
    db = mysql_db(container or os.environ.get("MYSQL_CONTAINER") or MYSQL_CONTAINER)
    require_db(db)
    harness = MysqlIntervalRun(
        rivet=resolve_rivet(rivet, debug_fallback=False),
        work=Path(work or os.environ.get("WORK") or "/tmp/rivet-cdc-interval"),
        db=db,
        url=url or os.environ.get("MYSQL_CDC_URL") or MYSQL_URL,
        phases=phases or os.environ.get("PHASES") or "10 2;20 2;30 2",
        skip_setup=_env_skip_setup() if skip_setup is None else skip_setup,
        tt=detect_time_tool(),
        strict=strict,
    )
    return harness.main()


def run_postgres(
    *,
    rivet: str | None = None,
    work: str | None = None,
    url: str | None = None,
    container: str | None = None,
    slot: str | None = None,
    phases: str | None = None,
    skip_setup: bool | None = None,
    strict: bool = False,
) -> int:
    """`dev/cdc_interval/run_pg.sh`."""
    require_oracle()
    shell.require("docker", hint="the source runs in a compose container")
    db = pg_db(container or os.environ.get("PG_CONTAINER") or PG_CONTAINER)
    require_db(db)
    harness = PostgresIntervalRun(
        rivet=resolve_rivet(rivet, debug_fallback=False),
        work=Path(work or os.environ.get("WORK") or "/tmp/rivet-cdc-interval-pg"),
        db=db,
        url=url or os.environ.get("PG_CDC_URL") or PG_URL,
        slot=slot or os.environ.get("SLOT") or "cdc_interval_pg",
        phases=phases or os.environ.get("PHASES") or "2 2;4 1;6 1",
        skip_setup=_env_skip_setup() if skip_setup is None else skip_setup,
        tt=detect_time_tool(),
        strict=strict,
    )
    return harness.main()


def _env_skip_setup() -> bool:
    """`[ "${SKIP_SETUP:-0}" = "1" ]` — the literal string 1, nothing else."""
    return os.environ.get("SKIP_SETUP", "0") == "1"


# ==== BASELINE + SOAK (baseline_mem.sh / soak_all.sh) ==========================
@contextlib.contextmanager
def fixtures(cleanup: Callable[[], None], *, keep_hint: str):
    """Run `cleanup` on the way out — on success AND on failure — but not on Ctrl-C.

    The bash called its cleanup only as the last statement of the happy path, so
    any failure mid-scenario leaked the fixture into the next one: a loaded table,
    an enabled MSSQL capture instance whose change table then grows unbounded, or
    a PostgreSQL slot pinning WAL on the source.

    Ctrl-C is exempt because an interrupted run's state IS the evidence (and, for
    a slot, dropping it would re-anchor the next run past the undrained tail).
    """
    try:
        yield
    except KeyboardInterrupt:
        shell.warn(f"interrupted — fixtures left in place. {keep_hint}")
        raise
    except BaseException:
        cleanup()
        raise
    else:
        cleanup()


def pin_run(rivet: Path, cfg: Path, label: str) -> None:
    """The `"$R" run -c … >/dev/null 2>&1` that pins the anchor and drains 0.

    The bash discarded its exit status. A failed pin means no slot / no binlog
    coordinate / no MSSQL from-LSN, so the drain later measures whatever anchor it
    creates for ITSELF — an events number that looks plausible and describes a
    backlog nobody pumped.
    """
    p = shell.run([str(rivet), "run", "-c", str(cfg)], timeout=None)
    if not p.ok:
        tail = (p.stderr or p.stdout).strip().splitlines()
        raise shell.Fail(
            f"{label}: the anchor-pinning run failed: "
            + (tail[-1] if tail else f"exit {p.returncode}"),
            hint=f"rivet run -c {cfg}",
        )


# ── baseline_mem.sh ────────────────────────────────────────────────────────────
def baseline_pg(*, n: int, batch: int, work: Path, rivet: Path, tt: TimeTool, log: Log) -> None:
    """PostgreSQL: hypothesis under test is that the slot peek is O(total backlog)."""
    db = pg_db()
    require_db(db)
    cfg, out, ckpt = work / "pg.yaml", work / "pg_out", work / "pg.ckpt"
    log.say("PG: setup")
    db.q(
        "SELECT pg_drop_replication_slot('bl_pg') FROM pg_replication_slots "
        "WHERE slot_name='bl_pg'"
    )
    db.sql(
        "DROP TABLE IF EXISTS bl; CREATE TABLE bl (id bigint primary key, "
        "v bigint not null, pad varchar(64) not null)"
    ).check("create bl")
    shell.rm_rf(ckpt)
    shell.rm_rf(out)
    out.mkdir(parents=True, exist_ok=True)
    shell.atomic_write(
        cfg,
        f"""source: {{ type: postgres, url: "{PG_URL}" }}
exports:
  - {{ name: bl, table: bl, mode: cdc, format: parquet, cdc: {{ checkpoint: "{ckpt}", slot: bl_pg, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{out}" }} }}
""",
    )

    def cleanup() -> None:
        db.q(
            "SELECT pg_drop_replication_slot('bl_pg') FROM pg_replication_slots "
            "WHERE slot_name='bl_pg'"
        )
        db.q("DROP TABLE IF EXISTS bl")

    with fixtures(cleanup, keep_hint="slot bl_pg is still pinning WAL; drop it with "
                  "SELECT pg_drop_replication_slot('bl_pg');"):
        pin_run(rivet, cfg, "PG")  # pin the slot, drain 0
        log.say(f"PG: pump {n} in {batch}-row txns")
        # MODERATE transactions on purpose: the question is memory-vs-BACKLOG, and
        # one giant transaction would confound it with the documented
        # O(largest transaction) bound. The procedure COMMITs every `batch` rows.
        db.sql(
            f"""CREATE OR REPLACE PROCEDURE bl_pump() LANGUAGE plpgsql AS $$
      DECLARE i bigint := 0;
      BEGIN WHILE i < {n} LOOP
        INSERT INTO bl SELECT g, g, '{pad("p_")}' FROM generate_series(i+1, LEAST(i+{batch},{n})) g;
        COMMIT; i := i + {batch}; END LOOP; END $$;""",
            timeout=None,
        ).check("create procedure bl_pump")
        db.sql("CALL bl_pump()", timeout=None).check("CALL bl_pump()")
        wal = db.q(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)"
            "::bigint/1048576 FROM pg_replication_slots WHERE slot_name='bl_pg'"
        )
        if not wal.strip().isdigit():
            raise shell.Fail(
                f"slot bl_pg reports no retained WAL ({wal!r}) after pumping {n} rows",
                hint="the pin run did not create the slot, so nothing was retained "
                "and the drain below would measure an empty backlog",
            )
        log.say(f"PG: drain (retained WAL={wal}MB)")
        d = drain(rivet, cfg, work, tt, log)
        print(
            f"RESULT postgres events={d.rows} wall={d.seconds}s rss={d.rss_mb}MB "
            f"retained_wal={wal}MB",
            flush=True,
        )


def baseline_mysql(*, n: int, batch: int, work: Path, rivet: Path, tt: TimeTool, log: Log) -> None:
    """MySQL: hypothesis is that streaming the binlog gives O(largest txn) — flat RSS."""
    db = mysql_db()
    require_db(db)
    cfg, out, ckpt = work / "my.yaml", work / "my_out", work / "my.ckpt"
    log.say("MySQL: setup")
    db.sql(
        "DROP TABLE IF EXISTS bl; CREATE TABLE bl (id bigint primary key, "
        "v bigint not null, pad varchar(64) not null)"
    ).check("create bl")
    db.sql("DROP TABLE IF EXISTS nums; CREATE TABLE nums (n int primary key)").check(
        "create nums"
    )
    db.sql(
        f"SET SESSION cte_max_recursion_depth=1000000; INSERT INTO nums (n) "
        f"WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < {batch}) "
        "SELECT n FROM s",
        timeout=None,
    ).check("fill nums")
    shell.rm_rf(ckpt)
    shell.rm_rf(out)
    out.mkdir(parents=True, exist_ok=True)
    shell.atomic_write(
        cfg,
        f"""source: {{ type: mysql, url: "{MYSQL_URL}" }}
exports:
  - {{ name: bl, table: bl, mode: cdc, format: parquet, cdc: {{ checkpoint: "{ckpt}", server_id: 49222, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{out}" }} }}
""",
    )

    def cleanup() -> None:
        db.q("DROP TABLE IF EXISTS bl, nums")

    with fixtures(cleanup, keep_hint="tables bl/nums remain in the mysql-cdc database."):
        pin_run(rivet, cfg, "MySQL")  # pin the binlog checkpoint at current, drain 0
        log.say(f"MySQL: pump {n} in {batch}-row txns")
        i = 0
        while i < n:
            # Each INSERT is its own autocommit transaction, so the largest
            # transaction is exactly `batch` rows.
            if not db.sql(
                f"INSERT INTO bl (id,v,pad) SELECT {i}+n, {i}+n, '{pad('p_')}' "
                f"FROM nums WHERE {i}+n <= {n}",
                timeout=None,
            ).ok:
                shell.warn(f"MySQL: pump insert at offset {i} failed")
            i += batch
        log.say("MySQL: drain")
        d = drain(rivet, cfg, work, tt, log)
        print(
            f"RESULT mysql events={d.rows} wall={d.seconds}s rss={d.rss_mb}MB "
            "retained_wal=n/a",
            flush=True,
        )


def baseline_mssql(*, n: int, batch: int, work: Path, rivet: Path, tt: TimeTool, log: Log) -> None:
    """SQL Server: polls the change-table window, so likely O(backlog) too."""
    db = mssql_db()
    require_db(db)
    cfg, out, ckpt = work / "ms.yaml", work / "ms_out", work / "ms.ckpt"
    log.say("MSSQL: setup + enable CDC")
    db.sql(
        "IF OBJECT_ID('dbo.bl') IS NOT NULL DROP TABLE dbo.bl; "
        "CREATE TABLE dbo.bl (id bigint primary key, v bigint not null, "
        "pad varchar(64) not null);"
    ).check("create dbo.bl")
    db.sql(
        "IF EXISTS (SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t "
        "ON ct.source_object_id=t.object_id WHERE t.name='bl')\n"
        "        EXEC sys.sp_cdc_disable_table @source_schema='dbo', "
        "@source_name='bl', @capture_instance='dbo_bl';"
    )
    db.sql(
        "EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='bl', "
        "@role_name=NULL, @capture_instance='dbo_bl', @supports_net_changes=0;"
    ).check("sp_cdc_enable_table dbo_bl")

    online_sql = (
        "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t "
        "ON ct.source_object_id=t.object_id WHERE t.name='bl'"
    )
    # Wait for the capture instance to come online (the Agent creates it).
    if not shell.wait_until(
        lambda: (db.first_int(online_sql) or 0) >= 1, tries=20, delay=2.0
    ):
        # The bash fell through silently here and drained a table CDC was never
        # capturing — a RESULT line of events=0 that reads like a rivet bug.
        raise shell.Fail(
            "MSSQL: capture instance dbo_bl never came online after 40s",
            hint="is SQL Server Agent running in the container? "
            "EXEC sys.sp_cdc_help_change_data_capture",
        )

    shell.rm_rf(ckpt)
    shell.rm_rf(out)
    out.mkdir(parents=True, exist_ok=True)
    shell.atomic_write(
        cfg,
        f"""source:
  type: mssql
  url: "{MSSQL_URL}"
  tls: {{ accept_invalid_certs: true }}
exports:
  - {{ name: bl, table: bl, mode: cdc, format: parquet, cdc: {{ checkpoint: "{ckpt}", capture_instance: dbo_bl, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{out}" }} }}
""",
    )

    def cleanup() -> None:
        db.q(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='bl', "
            "@capture_instance='dbo_bl'; DROP TABLE IF EXISTS dbo.bl;"
        )

    with fixtures(cleanup, keep_hint="capture instance dbo_bl is still enabled; its "
                  "change table keeps growing until you disable it."):
        pin_run(rivet, cfg, "MSSQL")  # pin the checkpoint at the current max LSN
        # Baseline the change-table count BEFORE pumping: stale rows below the
        # pinned LSN are never drained, so the Agent-extract wait must key on the
        # DELTA (before -> after), not an absolute count, or it fires early.
        ct0 = db.first_int("SELECT COUNT_BIG(*) FROM cdc.dbo_bl_CT") or 0
        log.say(f"MSSQL: pump {n} (cross-join, {batch}-row batches; CT baseline={ct0})")
        i = 0
        while i < n:
            if not db.sql(
                f"INSERT INTO dbo.bl (id,v,pad)\n"
                f"        SELECT TOP ({batch}) {i} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),\n"
                f"               {i} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '{pad('p_')}'\n"
                "        FROM sys.all_columns a CROSS JOIN sys.all_columns b;",
                timeout=None,
            ).ok:
                shell.warn(f"MSSQL: pump insert at offset {i} failed")
            i += batch

        target = ct0 + n
        log.say(f"MSSQL: wait for Agent to extract to CT>={target}")
        last = ct0
        for _ in range(120):
            last = db.first_int("SELECT COUNT_BIG(*) FROM cdc.dbo_bl_CT") or 0
            if last >= target:
                log.say(f"MSSQL: CT has {last} rows (delta {last - ct0})")
                break
            time.sleep(5)
        else:
            # The bash's loop ended silently, and the RESULT then understated the
            # backlog without saying why.
            shell.warn(
                f"MSSQL: Agent never reached CT>={target} in 600s (last={last}) — "
                "the drain below measures a PARTIAL backlog"
            )
            log.say(f"MSSQL: WARNING CT still at {last}, target was {target}")

        log.say("MSSQL: drain")
        d = drain(rivet, cfg, work, tt, log)
        print(
            f"RESULT mssql events={d.rows} wall={d.seconds}s rss={d.rss_mb}MB "
            "retained_wal=n/a",
            flush=True,
        )


BASELINE_TARGETS = {
    "pg": (baseline_pg,),
    "mysql": (baseline_mysql,),
    "mssql": (baseline_mssql,),
    # Same order as the bash's `all)` arm.
    "all": (baseline_mysql, baseline_pg, baseline_mssql),
}


def baseline_mem(
    target: str = "all",
    *,
    n: int | None = None,
    batch: int | None = None,
    work: str | None = None,
    rivet: str | None = None,
) -> int:
    """`dev/cdc_interval/baseline_mem.sh` — one big backlog, one drain, per engine."""
    only = target or "all"
    if only not in BASELINE_TARGETS:
        # The bash printed exactly this and exited 1 (its `case` DID have a
        # default arm — soak_all.sh's did not).
        print(f"unknown target: {only}")
        return 1
    shell.require("docker", hint="the sources run in compose containers")
    rivet_path = resolve_rivet(rivet, debug_fallback=True)
    n_rows = int(n if n is not None else os.environ.get("N", 1000000))
    batch_rows = int(batch if batch is not None else os.environ.get("BATCH", 5000))
    work_dir = Path(work or os.environ.get("WORK") or "/tmp/rivet-cdc-baseline")
    work_dir.mkdir(parents=True, exist_ok=True)
    tt = detect_time_tool()
    log = Log()  # baseline_mem.sh's say() writes stdout only — no log file
    log.say(
        f"=== CDC memory baseline: N={n_rows}, batch={batch_rows}, "
        f"bin={rivet_path}, only={only} ==="
    )
    for fn in BASELINE_TARGETS[only]:
        fn(n=n_rows, batch=batch_rows, work=work_dir, rivet=rivet_path, tt=tt, log=log)
    log.say("=== baseline complete ===")
    return 0


# ── soak_all.sh ────────────────────────────────────────────────────────────────
SOAK_ENGINES = ("pg", "mysql", "mssql", "mongo")
SOAK_USAGE = "usage: soak-all <pg|mysql|mssql|mongo>"
# The last (largest-backlog) drain must not exceed 1.5x the first. Integer
# arithmetic, exactly as the bash's `$(( FIRST_RSS * 3 / 2 ))`: a leak or an
# O(backlog) regression blows well past 1.5x over a 12x backlog growth
# (10 -> 120 min), while normal allocator noise stays far below it.
RSS_TREND_NUMERATOR, RSS_TREND_DENOMINATOR = 3, 2


class Soak:
    """One engine, growing intervals, bounded drain — `soak_all.sh`.

    Post-fix validation of two properties at once:
      (a) peak drain RSS stays FLAT as the per-interval backlog grows ~12x — a
          leak or an O(backlog) regression shows up as RSS tracking the interval;
      (b) nothing is lost: source COUNT(*) == distinct inserted ids in the parts.
    """

    def __init__(
        self,
        *,
        engine: str,
        rivet: Path,
        work: Path,
        phases_raw: str,
        batch: int,
        tick: int,
        tt: TimeTool,
        strict: bool,
    ) -> None:
        if engine not in SOAK_ENGINES:
            # soak_all.sh's per-engine `case` statements have NO default arm, so an
            # unknown engine ran the whole multi-hour soak creating nothing,
            # inserting nothing and "measuring" it.
            raise shell.Fail(f"unknown engine: {engine}", hint=SOAK_USAGE)
        self.engine = engine
        self.rivet = rivet
        self.work = work
        self.out = work / "out"
        self.cfg = work / "cdc.yaml"
        self.ckpt = work / "cdc.ckpt"
        self.next_file = work / "next"
        self.phases_raw = phases_raw
        self.phases = phases_minutes(phases_raw)
        self.batch = batch
        self.tick = tick
        self.tt = tt
        self.strict = strict
        self.pad = pad("s_")
        self.log = Log(work / "soak.log", prefix=f"[{engine}] ")
        self.stop = Stop(work / "churn.stop")
        self.pause = Flag(work / "churn.pause")
        self.db = {
            "pg": pg_db,
            "mysql": mysql_db,
            "mssql": mssql_db,
            "mongo": mongo_db,
        }[engine]()
        self._workers: list[threading.Thread] = []

    # ── per-engine SQL ─────────────────────────────────────────────────────────
    def setup(self) -> None:
        shell.rm_rf(self.out)
        self.out.mkdir(parents=True, exist_ok=True)
        shell.rm_rf(self.ckpt)
        shell.atomic_write(self.next_file, "1\n")
        if self.engine == "pg":
            self.db.q(
                "SELECT pg_drop_replication_slot('soak_pg') FROM pg_replication_slots "
                "WHERE slot_name='soak_pg'"
            )
            self.db.sql(
                "DROP TABLE IF EXISTS sk; CREATE TABLE sk (id bigint primary key, "
                "v bigint not null, pad varchar(64) not null)"
            ).check("create sk")
            cfg = f"""source: {{ type: postgres, url: "{PG_URL}" }}
exports:
  - {{ name: sk, table: sk, mode: cdc, format: parquet, cdc: {{ checkpoint: "{self.ckpt}", slot: soak_pg, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{self.out}" }} }}
"""
        elif self.engine == "mysql":
            self.db.sql(
                "DROP TABLE IF EXISTS sk; CREATE TABLE sk (id bigint primary key, "
                "v bigint not null, pad varchar(64) not null)"
            ).check("create sk")
            cfg = f"""source: {{ type: mysql, url: "{MYSQL_URL}" }}
exports:
  - {{ name: sk, table: sk, mode: cdc, format: parquet, cdc: {{ checkpoint: "{self.ckpt}", server_id: 49333, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{self.out}" }} }}
"""
        elif self.engine == "mssql":
            self.db.sql(
                "IF OBJECT_ID('dbo.sk') IS NOT NULL DROP TABLE dbo.sk; "
                "CREATE TABLE dbo.sk (id bigint primary key, v bigint not null, "
                "pad varchar(64) not null);"
            ).check("create dbo.sk")
            self.db.sql(
                "IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_sk') "
                "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='sk',"
                "@capture_instance='dbo_sk';"
            )
            self.db.sql(
                "EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='sk',"
                "@role_name=NULL,@capture_instance='dbo_sk',@supports_net_changes=0;"
            ).check("sp_cdc_enable_table dbo_sk")
            if not shell.wait_until(
                lambda: (
                    self.db.first_int(
                        "SELECT COUNT(*) FROM cdc.change_tables "
                        "WHERE capture_instance='dbo_sk'"
                    )
                    or 0
                )
                >= 1,
                tries=20,
                delay=2.0,
            ):
                raise shell.Fail(
                    "MSSQL: capture instance dbo_sk never came online after 40s",
                    hint="is SQL Server Agent running in the container?",
                )
            cfg = f"""source:
  type: mssql
  url: "{MSSQL_URL}"
  tls: {{ accept_invalid_certs: true }}
exports:
  - {{ name: sk, table: sk, mode: cdc, format: parquet, cdc: {{ checkpoint: "{self.ckpt}", capture_instance: dbo_sk, until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{self.out}" }} }}
"""
        elif self.engine == "mongo":
            self.db.q("db.sk.drop()")
            cfg = f"""source: {{ type: mongo, url: "{MONGO_URL}" }}
exports:
  - {{ name: sk, table: sk, mode: cdc, format: parquet, cdc: {{ checkpoint: "{self.ckpt}", until_current: true, rollover: {ROLLOVER} }}, destination: {{ type: local, path: "{self.out}" }} }}
"""
        else:  # unreachable: the constructor validates. Present because the bash's
            # missing default arm is exactly how a store got reported UP unprobed.
            raise shell.Fail(f"unknown engine: {self.engine}", hint=SOAK_USAGE)
        shell.atomic_write(self.cfg, cfg)
        pin_run(self.rivet, self.cfg, self.engine)  # pin the anchor, drain 0

    def insert_rows(self, a: int, b: int) -> shell.Proc:
        if self.engine == "pg":
            return self.db.sql(
                f"INSERT INTO sk SELECT g,g,'{self.pad}' FROM generate_series({a},{b}) g",
                timeout=None,
            )
        if self.engine == "mysql":
            return self.db.sql(
                "SET SESSION cte_max_recursion_depth=1000000; "
                f"INSERT INTO sk (id,v,pad) SELECT n,n,'{self.pad}' FROM "
                f"(WITH RECURSIVE s(n) AS (SELECT {a} UNION ALL SELECT n+1 FROM s "
                f"WHERE n < {b}) SELECT n FROM s) q",
                timeout=None,
            )
        if self.engine == "mssql":
            return self.db.sql(
                f"SET NOCOUNT ON; INSERT INTO dbo.sk (id,v,pad) SELECT TOP ({b - a + 1}) "
                f"{a - 1}+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), "
                f"{a - 1}+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '{self.pad}' "
                "FROM sys.all_columns x CROSS JOIN sys.all_columns y;",
                timeout=None,
            )
        if self.engine == "mongo":
            return self.db.sql(
                f"var d=[]; for(var i={a};i<={b};i++){{d.push({{_id:i,v:i,"
                f"pad:'{self.pad}'}}); if(d.length===5000){{db.sk.insertMany(d);"
                "d.length=0;}} if(d.length)db.sk.insertMany(d)",
                timeout=None,
            )
        raise shell.Fail(f"unknown engine: {self.engine}", hint=SOAK_USAGE)

    def src_count(self) -> str:
        """Source truth, as a STRING — it is compared to duckdb's rendering with
        `=`, exactly as the bash did, so an empty result mismatches loudly."""
        if self.engine in ("pg", "mysql"):
            return self.db.q("SELECT COUNT(*) FROM sk", timeout=None)
        if self.engine == "mssql":
            v = self.db.first_int("SELECT COUNT_BIG(*) FROM dbo.sk", timeout=None)
            return "" if v is None else str(v)
        if self.engine == "mongo":
            v = self.db.first_int("print(db.sk.countDocuments())", timeout=None)
            return "" if v is None else str(v)
        raise shell.Fail(f"unknown engine: {self.engine}", hint=SOAK_USAGE)

    def dest_distinct(self) -> str:
        """A single-table export writes its parts to the destination path
        directly. The key column is `id` for the SQL engines and `_id` for
        Mongo's JSON-blob model."""
        if not any(self.out.glob("cdc-*.parquet")):
            return "0"
        key = '"_id"' if self.engine == "mongo" else "id"
        return duckdb_csv(
            f"SELECT COUNT(DISTINCT {key}) FROM read_parquet('{self.out}/cdc-*.parquet') "
            "WHERE __op='insert'",
            log=None,  # the bash sent this one's stderr to /dev/null
        )

    def mssql_agent_catchup(self) -> None:
        """MSSQL only: the Agent extracts into the change table asynchronously, so
        a drain fired too early sees a short backlog and looks like data loss."""
        want = self.src_count()
        target = int(want) if want.isdigit() else 0
        for _ in range(60):
            if (self.db.first_int("SELECT COUNT_BIG(*) FROM cdc.dbo_sk_CT") or 0) >= target:
                return
            time.sleep(3)
        shell.warn(
            f"MSSQL: Agent did not reach CT>={target} in 180s — the drain below may "
            "measure a partial backlog and report a COUNT mismatch"
        )

    def cleanup(self, *, keep: bool) -> None:
        if keep:
            shell.warn(
                f"interrupted — {self.engine} fixture kept "
                f"(table sk / collection sk, checkpoint {self.ckpt})"
                + (
                    "; slot soak_pg is STILL PINNING WAL — drop it with SELECT "
                    "pg_drop_replication_slot('soak_pg');"
                    if self.engine == "pg"
                    else ""
                )
            )
            return
        if self.engine == "pg":
            self.db.q(
                "SELECT pg_drop_replication_slot('soak_pg') FROM pg_replication_slots "
                "WHERE slot_name='soak_pg'"
            )
            self.db.q("DROP TABLE IF EXISTS sk")
        elif self.engine == "mysql":
            self.db.q("DROP TABLE IF EXISTS sk")
        elif self.engine == "mssql":
            self.db.q(
                "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='sk',"
                "@capture_instance='dbo_sk'; DROP TABLE IF EXISTS dbo.sk;"
            )
        elif self.engine == "mongo":
            self.db.q("db.sk.drop()")

    # ── churn ──────────────────────────────────────────────────────────────────
    def read_next(self) -> int:
        text = self.next_file.read_text().strip()
        if not text.isdigit():
            raise shell.Fail(f"{self.next_file} does not hold a number: {text!r}")
        return int(text)

    def churner(self) -> None:
        try:
            while not self.stop.stopped:
                if self.pause.is_set:
                    if self.stop.wait(1):
                        break
                    continue
                a = self.read_next()
                b = a + self.batch - 1
                if not self.insert_rows(a, b).ok:
                    # The bash discarded this status. A churner whose inserts all
                    # fail makes src_count == dest_distinct == 0 and the soak
                    # reports a flat-RSS PASS over an idle database.
                    shell.bad(f"churn insert {a}..{b} failed")
                    self.log.say(f"churn: insert @{a} failed")
                shell.atomic_write(self.next_file, f"{b + 1}\n")
                self.stop.wait(self.tick)
        except BaseException as e:  # noqa: BLE001
            shell.bad(f"churner thread died ({e!r}) — later intervals carry NO load")
            self.log.say(f"churner ABORTED: {e!r}")

    def join_workers(self) -> None:
        for t in self._workers:
            t.join(timeout=self.tick + 10)
        self._workers = [t for t in self._workers if t.is_alive()]

    # ── driver ─────────────────────────────────────────────────────────────────
    def main(self) -> int:
        self.work.mkdir(parents=True, exist_ok=True)
        self.stop.clear()
        self.pause.clear()
        self.log.say(
            f"=== soak start: phases='{self.phases_raw}' min, churn "
            f"{self.batch}/{self.tick}s, bin={self.rivet} ==="
        )
        first_rss: int | None = None
        last_rss: int | None = None
        verdict = "OK"
        interrupted = False
        try:
            # setup() is INSIDE the guard: it enables the MSSQL capture instance
            # and creates the PostgreSQL slot, so a failure between those and the
            # first interval would otherwise leave a growing change table / a
            # WAL-pinning slot behind (the bash cleaned up only on its happy path).
            self.setup()
            self._workers.append(spawn(self.churner, "churner"))
            self.log.say(f"churner pid={os.getpid()} (in-process thread)")
            for mins in self.phases:
                self.log.say(f"--- interval {mins}min (accumulating backlog) ---")
                time.sleep(mins * 60)
                self.pause.set()
                time.sleep(3)  # quiesce so source count == what will drain
                if self.engine == "mssql":
                    self.mssql_agent_catchup()
                d = drain(self.rivet, self.cfg, self.work, self.tt, self.log)
                sc = self.src_count()
                dd = self.dest_distinct()
                local_ok = "OK" if sc == dd else f"COUNT({sc}!={dd})"
                if local_ok != "OK":
                    verdict = "FAIL"
                self.pause.clear()
                if first_rss is None:
                    first_rss = d.rss_mb
                last_rss = d.rss_mb
                self.log.say(
                    f"RESULT interval={mins}min events={d.rows} rss={d.rss_mb}MB "
                    f"src={sc} dest_distinct={dd} => {local_ok}"
                )
            self.stop.set()
            self.join_workers()

            trend = "OK"
            if (
                first_rss is not None
                and last_rss is not None
                and last_rss > first_rss * RSS_TREND_NUMERATOR // RSS_TREND_DENOMINATOR
            ):
                trend = "RSS-TREND-FAIL"
                verdict = "FAIL"
            self.log.say(
                f"=== soak done: verdict={verdict} | RSS first={first_rss}MB "
                f"last={last_rss}MB ({trend}) ==="
            )
        except KeyboardInterrupt:
            interrupted = True
            raise
        finally:
            self.stop.set()
            self.join_workers()
            self.pause.clear()
            self.cleanup(keep=interrupted)
        return 1 if (self.strict and verdict != "OK") else 0


def soak_all(
    engine: str | None = None,
    *,
    phases: str | None = None,
    batch: int | None = None,
    tick: int | None = None,
    work: str | None = None,
    rivet: str | None = None,
    strict: bool = False,
) -> int:
    """`dev/cdc_interval/soak_all.sh` — one engine per invocation.

    The bash's own usage note says to parallelise ACROSS engines from the caller
    (`soak_all.sh pg > pg.soak.log 2>&1 &`), and it neither called nor was called
    by the other three scripts. Exposed as a function so a caller can compose the
    four in-process instead of re-shelling the retired `.sh` files.
    """
    if not engine:
        raise shell.Fail(SOAK_USAGE)
    require_oracle()
    shell.require("docker", hint="the sources run in compose containers")
    default_work = f"/tmp/rivet-cdc-soak/{engine}"
    soak = Soak(
        engine=engine,
        rivet=resolve_rivet(rivet, debug_fallback=True),
        work=Path(work or os.environ.get("WORK") or default_work),
        phases_raw=phases or os.environ.get("PHASES") or "10 20 30 60 120",
        batch=int(batch if batch is not None else os.environ.get("BATCH", 2000)),
        tick=int(tick if tick is not None else os.environ.get("TICK", 5)),
        tt=detect_time_tool(),
        strict=strict,
    )
    require_db(soak.db)
    return soak.main()


# ==== CLI ======================================================================
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cdc_interval",
        description="CDC drain-interval and drain-memory harnesses "
        "(ports of dev/cdc_interval/*.sh).",
    )
    sub = p.add_subparsers(dest="cmd")

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--rivet-bin", dest="rivet", help="default: $RIVET_BIN or target/release/rivet")
        sp.add_argument("--work", help="scratch directory (default: $WORK or the script's /tmp path)")
        sp.add_argument("--phases", help="default: $PHASES or the script's own schedule")

    r = sub.add_parser("run", help="MySQL drain-interval experiment (run.sh)")
    common(r)
    r.add_argument("--url", help="default: $MYSQL_CDC_URL")
    r.add_argument("--container", help="default: $MYSQL_CONTAINER")
    r.add_argument("--skip-setup", action="store_true", default=None,
                   help="resume: keep the existing tables/checkpoint ($SKIP_SETUP=1)")
    r.add_argument("--strict", action="store_true",
                   help="exit 1 if any verdict is not OK (the bash always exited 0)")

    g = sub.add_parser("run-pg", help="PostgreSQL drain-interval experiment (run_pg.sh)")
    common(g)
    g.add_argument("--url", help="default: $PG_CDC_URL")
    g.add_argument("--container", help="default: $PG_CONTAINER")
    g.add_argument("--slot", help="default: $SLOT or cdc_interval_pg")
    g.add_argument("--skip-setup", action="store_true", default=None)
    g.add_argument("--strict", action="store_true")

    b = sub.add_parser("baseline-mem", help="peak drain RSS vs backlog (baseline_mem.sh)")
    b.add_argument("target", nargs="?", default="all", choices=None,
                   help="pg | mysql | mssql | all (default: all)")
    b.add_argument("--rivet-bin", dest="rivet")
    b.add_argument("--work")
    b.add_argument("-n", "--rows", dest="n", type=int, help="default: $N or 1000000")
    b.add_argument("--batch", type=int, help="rows per transaction (default: $BATCH or 5000)")

    s = sub.add_parser("soak-all", help="growing-interval memory soak, one engine (soak_all.sh)")
    s.add_argument("engine", nargs="?", help="pg | mysql | mssql | mongo")
    common(s)
    s.add_argument("--batch", type=int, help="rows per churn tick (default: $BATCH or 2000)")
    s.add_argument("--tick", type=int, help="seconds between churn ticks (default: $TICK or 5)")
    s.add_argument("--strict", action="store_true")
    return p


def _sigterm_as_interrupt() -> None:
    """Make `kill <pid>` behave like Ctrl-C.

    These harnesses run detached for hours, so `kill` — not Ctrl-C — is how they
    actually get stopped, and a bare SIGTERM kills the interpreter without running
    any `finally`. That silently leaves a PostgreSQL slot pinning WAL on the
    source. Turning it into KeyboardInterrupt takes the documented
    keep-the-fixtures path, which at least PRINTS the slot and how to drop it.
    Installed only from the CLI, so importing this module changes nothing.
    """
    import signal

    def handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    with contextlib.suppress(ValueError, OSError):  # not the main thread / unsupported
        signal.signal(signal.SIGTERM, handler)


def main_cli(argv: list[str] | None = None) -> int:
    _sigterm_as_interrupt()
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.cmd == "run":
        return run_mysql(
            rivet=args.rivet, work=args.work, url=args.url, container=args.container,
            phases=args.phases, skip_setup=args.skip_setup, strict=args.strict,
        )
    if args.cmd == "run-pg":
        return run_postgres(
            rivet=args.rivet, work=args.work, url=args.url, container=args.container,
            slot=args.slot, phases=args.phases, skip_setup=args.skip_setup,
            strict=args.strict,
        )
    if args.cmd == "baseline-mem":
        return baseline_mem(
            args.target, n=args.n, batch=args.batch, work=args.work, rivet=args.rivet
        )
    if args.cmd == "soak-all":
        return soak_all(
            args.engine, phases=args.phases, batch=args.batch, tick=args.tick,
            work=args.work, rivet=args.rivet, strict=args.strict,
        )
    parser.print_help()
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
