"""CDC test stand + capture-throughput matrix.

Port of two bash scripts that share one subject — the `cdc` docker profile:

* `dev/cdc/stand.sh`  — bring the four CDC engines up, verify each is actually
  CDC-*ready*, run the scenario suites, tear the profile down.
* `dev/cdc/perf.sh`   — the bulk-backlog throughput matrix: per engine, pin the
  resume anchor, seed N rows, run ONE bounded (`until_current`) drain, and
  report wall / rows / rows-per-second / peak RSS.

They live in one module because `perf` is a *consumer* of the stand: the same
four containers, the same per-engine CDC prerequisites, the same SQL wrappers.
Splitting them would duplicate the plumbing (and the plumbing is where the bash
drifted).

The stand is `postgres-cdc:5434` / `mysql-cdc:3307` / `mssql-cdc:1434` /
`mongo-rs:27018`, deliberately on other ports than the batch stack so both can
run at once. The scenario coverage map lives in `dev/cdc/README.md`.

Usage (mirrors the bash subcommands):

    python3 dev/pytools/cdc_stand.py up            # compose up + verify ready
    python3 dev/pytools/cdc_stand.py verify        # re-check CDC readiness
    python3 dev/pytools/cdc_stand.py scenarios     # live_cdc suites (serial)
    python3 dev/pytools/cdc_stand.py standby       # primary+replica fail-loud
    python3 dev/pytools/cdc_stand.py perf [engines…] [--n N] [--work DIR] [--bin P]
    python3 dev/pytools/cdc_stand.py harm [args…]  # delegates to dev/cdc/harm.py
    python3 dev/pytools/cdc_stand.py soak <engine> [phases…]
    python3 dev/pytools/cdc_stand.py all           # up + scenarios + perf
    python3 dev/pytools/cdc_stand.py down

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH (each one a bug it shipped):

1. `perf` cleaned up (drop slot / table / capture instance) only on the happy
   path — a failed seed or an interrupted drain leaked the captured table, the
   `perf_pg` slot (which then pins WAL forever) and the `dbo_pf` capture
   instance into the next run. Every engine's body is now `try/finally`.
2. `perf` passed `/usr/bin/time -l` unconditionally. That is the BSD/macOS flag;
   GNU time wants `-v` and exits 1 before the binary even runs, so the whole
   matrix was macOS-only. Split by platform, exactly as `scripts/soak_cdc.sh`
   already did after the same finding.
3. The elapsed/RSS parse is shared and unit-aware: BSD reports max RSS in BYTES,
   GNU in KILOBYTES, and GNU renders elapsed as `h:mm:ss`/`m:ss` — see
   `parse_time_report`, which sums the colon fields instead of reading the last
   one (this repo has already shipped a parser that read `0:01:23.45` as 23.45,
   turning a 90 s run into a 30 s one).
4. The health-gate counted `docker inspect | grep -c healthy`, and "unhealthy"
   CONTAINS "healthy" — four unhealthy containers satisfied `n -ge 4`. The count
   here is an exact match.
5. An unknown engine name hit a `case` with no default arm: nothing was set up,
   the drain ran against a config that was never written, and the engine's row
   came out as zeros. Now it is a loud `Fail`.
6. `perf` needs `duckdb` to count the drained rows; without it the rows column
   was blank and rows/s 0 — indistinguishable from a capture that dropped
   everything. Now a named precondition.
7. A missing rivet binary fell back to `target/debug/rivet` whether or not that
   existed, then produced a table of zeros. Now a `Fail` naming `cargo build`.
"""

from __future__ import annotations

import os
import platform
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/cdc_stand.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT

# ── the stand ──────────────────────────────────────────────────────────────────
CDC_SVCS: tuple[str, ...] = ("postgres-cdc", "mysql-cdc", "mssql-cdc", "mongo-rs")
# A few scenarios capture CDC straight into a cloud destination (e.g.
# cdc_multi_table_to_gcs_lands_per_table_prefixes), so the object stores come up
# with the stand — otherwise those scenarios skip and the coverage silently thins.
CLOUD_SVCS: tuple[str, ...] = ("minio", "fake-gcs")

PGC = "rivet-postgres-cdc-1"
MYC = "rivet-mysql-cdc-1"
MSC = "rivet-mssql-cdc-1"
MOC = "rivet-mongo-rs-1"
STANDBY_C = "rivet-pg-cdc-standby-1"

SA_PASSWORD = "Rivet_Passw0rd!"
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"

USAGE = (
    "usage: dev/pytools/cdc_stand.py "
    "<up|verify|scenarios|standby|perf [engines…]|harm [args…]|soak <engine>|all|down>"
)

PERF_ENGINES: tuple[str, ...] = ("pg", "mysql", "mssql", "mongo")


def say(msg: str) -> None:
    """The bash `say()` — `[cdc-stand] …`, on stderr so a piped stdout stays data."""
    shell.log(msg, tag="cdc-stand")


# ── compose ────────────────────────────────────────────────────────────────────
# The bash probed for the v2 plugin (`docker compose`) and fell back to the
# standalone binary (`docker-compose`, still what the devbox has). Keep both, and
# keep the refusal when neither exists — `shell.compose` only knows the v2 form.
_DC: list[str] | None = None


def _dc_prefix() -> list[str]:
    global _DC
    if _DC is None:
        if shell.run(["docker", "compose", "version"]).ok:
            _DC = ["docker", "compose"]
        elif shell.have("docker-compose"):
            _DC = ["docker-compose"]
        else:
            raise shell.Fail("neither 'docker compose' nor 'docker-compose' is available")
    return list(_DC)


def dc(*args: str, show: bool = False, timeout: float | None = 600) -> shell.Proc:
    """A compose call, always from the repo root so the compose file (plus the
    gitignored `docker-compose.override.yaml` the dev stand relies on) is found
    regardless of the caller's cwd — the bash `cd`'d to the git toplevel for
    exactly this reason."""
    argv = [*_dc_prefix(), *args]
    runner = shell.stream if show else shell.run
    return runner(argv, cwd=ROOT, timeout=timeout)


# ── per-engine argv builders (shared with cdc_soak) ────────────────────────────
def psql_argv(query: str, *, db: str | None = "rivet") -> list[str]:
    argv = ["psql", "-U", "rivet"]
    if db:
        argv += ["-d", db]
    return [*argv, "-tAc", query]


def mysql_argv(query: str, *, db: str | None = "rivet") -> list[str]:
    """`-N` drops the column headers; the db is positional and ABSENT in the
    readiness probe (`SELECT @@binlog_format` needs no database)."""
    argv = ["mysql", "-urivet", "-privet"]
    if db:
        argv.append(db)
    return [*argv, "-N", "-e", query]


def sqlcmd_argv(
    query: str, *, db: str | None = "rivet", wide: bool = False, nocount: bool = False
) -> list[str]:
    """`-C` trusts the container's self-signed cert, `-h -1` drops the header
    rules. `nocount` prepends `SET NOCOUNT ON;` so the `(N rows affected)` noise
    does not land in the value we are about to parse."""
    argv = [SQLCMD, "-C", "-S", "localhost", "-U", "sa", "-P", SA_PASSWORD]
    if db:
        argv += ["-d", db]
    argv += ["-h", "-1"]
    if wide:
        argv.append("-W")
    return [*argv, "-Q", f"SET NOCOUNT ON; {query}" if nocount else query]


def mongosh_argv(script: str, *, db: str | None = None) -> list[str]:
    """Port 27017 INSIDE the container — 27018 is the published mapping."""
    argv = ["mongosh", "--quiet", "--port", "27017"]
    if db:
        argv.append(db)
    return [*argv, "--eval", script]


def pg_sql(query: str, *, container: str = PGC, timeout: float | None = 600) -> shell.Proc:
    return shell.docker_exec(container, *psql_argv(query), timeout=timeout)


def my_sql(
    query: str, *, db: str | None = "rivet", timeout: float | None = 600
) -> shell.Proc:
    return shell.docker_exec(MYC, *mysql_argv(query, db=db), timeout=timeout)


def ms_sql(
    query: str,
    *,
    db: str | None = "rivet",
    nocount: bool = False,
    timeout: float | None = 600,
) -> shell.Proc:
    return shell.docker_exec(
        MSC, *sqlcmd_argv(query, db=db, nocount=nocount), timeout=timeout
    )


def mo_sql(
    script: str, *, db: str | None = "perfdb", timeout: float | None = 600
) -> shell.Proc:
    return shell.docker_exec(MOC, *mongosh_argv(script, db=db), timeout=timeout)


def squash(text: str) -> str:
    """`tr -d '[:space:]'` — sqlcmd pads its single-column output and mongosh may
    wrap, so the verdict has to compare the value, not the layout."""
    return "".join(text.split())


def first_int(text: str) -> int | None:
    """The bash `grep -Eo '[0-9]+' | head -1`: the first integer anywhere in the
    output. Returns None when there is none — an ERROR message has no digits, and
    treating "no digits" as 0 is how a dead Agent reads as "not caught up yet"
    rather than as a failure."""
    m = re.search(r"[0-9]+", text)
    return int(m.group(0)) if m else None


# ── rivet binary ───────────────────────────────────────────────────────────────
def resolve_bin(
    explicit: str | Path | None = None,
    *,
    env: str = "RIVET_BIN",
    default: str = "target/release/rivet",
    fallback: str | None = "target/debug/rivet",
) -> Path:
    """`RIVET_BIN` / release / debug, in the bash's order — but a `Fail` when
    none of them is executable.

    The bash assigned the debug path unconditionally and only found out at exec
    time, per engine, with the error swallowed by `|| true`: the matrix printed a
    full table of zeros that looked like a capture bug.
    """
    tried: list[Path] = []
    for cand in (explicit or os.environ.get(env) or default, fallback):
        if cand is None:
            continue
        p = Path(cand)
        if not p.is_absolute():
            p = ROOT / p
        tried.append(p)
        if p.is_file() and os.access(p, os.X_OK):
            return p
        if explicit:  # an explicitly named binary is not silently replaced
            break
    raise shell.Fail(
        f"no executable rivet binary at {' or '.join(str(t) for t in tried)}",
        hint="cargo build --release  (a debug build gives unrepresentative numbers)",
    )


# ── /usr/bin/time ──────────────────────────────────────────────────────────────
def time_flag() -> str:
    """BSD/macOS `-l` vs GNU/Linux `-v`. Getting this wrong is not a degraded
    measurement, it is exit 1 before the measured binary ever starts."""
    return "-l" if platform.system() == "Darwin" else "-v"


_BSD_REAL = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s+real\b", re.M)
# The GNU label CONTAINS colons — "Elapsed (wall clock) time (h:mm:ss or m:ss):" —
# and so does the value. A greedy `.*:` therefore captures the last field of the
# value ("23.45" out of "0:01:23.45"), which is precisely the minutes-dropping
# parser this repo already shipped once. Non-greedy up to a digit-led token that
# runs to end-of-line captures the WHOLE value.
_GNU_ELAPSED = re.compile(r"Elapsed \(wall clock\) time[^\n]*?:[ \t]*([0-9][0-9:.]*)[ \t]*$", re.M)
_BSD_RSS = re.compile(r"^\s*([0-9]+)\s+maximum resident set size", re.M | re.I)
_GNU_RSS = re.compile(r"maximum resident set size \(kbytes\):\s*([0-9]+)", re.I)


def parse_hms(value: str) -> float:
    """`h:mm:ss(.ff)` / `m:ss(.ff)` / `ss(.ff)` → seconds.

    Sum the colon-separated fields base-60 instead of picking one. A previous
    parser in this repo took the LAST field, so `0:01:23.45` measured as 23.45 —
    a 90 s run reported as 30 s, i.e. a 3× throughput lie in the flattering
    direction. Anything unparseable raises, so the caller falls back to a clock
    it trusts rather than publishing a number it invented.
    """
    total = 0.0
    for field in value.split(":"):
        total = total * 60.0 + float(field)
    return total


def parse_time_report(text: str) -> tuple[float | None, int | None]:
    """`(elapsed_seconds, peak_rss_bytes)` from either dialect of `/usr/bin/time`.

    RSS is normalised to BYTES: BSD prints bytes ("NNN  maximum resident set
    size"), GNU prints kilobytes ("Maximum resident set size (kbytes): NNN").
    The bash divided by 1048576 in both cases, so on Linux every engine would
    have reported 0 MB.
    """
    elapsed: float | None = None
    m = _BSD_REAL.search(text)
    if m:
        elapsed = float(m.group(1))
    else:
        m = _GNU_ELAPSED.search(text)
        if m:
            try:
                elapsed = parse_hms(m.group(1))
            except ValueError:
                elapsed = None

    rss: int | None = None
    m = _GNU_RSS.search(text)
    if m:
        rss = int(m.group(1)) * 1024
    else:
        m = _BSD_RSS.search(text)
        if m:
            rss = int(m.group(1))
    return elapsed, rss


@dataclass
class Timed:
    proc: shell.Proc
    seconds: float
    rss_bytes: int | None


def timed_run(argv: Sequence[str], report_path: Path) -> Timed:
    """Run `argv` under `/usr/bin/time`, keeping the report on disk (the bash kept
    `$WORK/$e.time`; it is the only evidence left when a drain fails).

    Wall time is measured here as well and WINS when the report cannot be
    parsed — a missing number must not silently become the `${REAL:-1}` the bash
    substituted into the rows/s denominator.
    """
    full = ["/usr/bin/time", time_flag(), *[str(a) for a in argv]]
    t0 = time.monotonic()
    p = shell.run(full, cwd=ROOT, timeout=None)
    wall = time.monotonic() - t0
    # `/usr/bin/time` writes its report to stderr, mixed with the child's stderr.
    report = p.stderr
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)
    parsed, rss = parse_time_report(report)
    return Timed(p, parsed if parsed is not None else wall, rss)


# ── duckdb ─────────────────────────────────────────────────────────────────────
def duckdb_scalar(query: str) -> str | None:
    """One scalar via the duckdb CLI, or None when the query fails.

    The independent reader is the point: counting the drained rows with rivet's
    own manifest would make the check a self-oracle.
    """
    p = shell.run(["duckdb", "-noheader", "-csv", "-c", query], cwd=ROOT, timeout=None)
    if not p.ok:
        return None
    value = p.stdout.strip()
    return value or None


# ══ stand.sh ═══════════════════════════════════════════════════════════════════
def healthy_count() -> int:
    """How many of the four report `healthy`.

    Exact equality, not a substring: `grep -c healthy` also matches *un*healthy,
    so the bash gate was satisfied by four unhealthy containers and handed the
    scenarios a stand that was not up.
    """
    p = shell.docker("inspect", "-f", "{{.State.Health.Status}}", PGC, MYC, MSC, MOC)
    return sum(1 for line in p.stdout.splitlines() if line.strip() == "healthy")


def up() -> int:
    say(f"compose --profile cdc up -d ({' '.join(CDC_SVCS)})")
    if not dc("--profile", "cdc", "up", "-d", *CDC_SVCS, show=True, timeout=None).ok:
        raise shell.Fail("compose up failed")

    say(f"bringing up the cloud-destination stores ({' '.join(CLOUD_SVCS)}) for CDC→cloud tests")
    if not dc("up", "-d", *CLOUD_SVCS, timeout=None).ok:
        shell.warn(
            f"warning: could not start {' '.join(CLOUD_SVCS)} "
            "(CDC→cloud scenarios will be skipped)"
        )

    say("waiting for containers to be healthy...")
    if not shell.wait_until(lambda: healthy_count() >= 4, tries=60, delay=3.0):
        # The bash fell through silently here; `verify()` below is the real gate,
        # but say so, because "verify failed" reads very differently from
        # "verify failed after the health wait timed out".
        shell.warn(f"only {healthy_count()}/4 containers report healthy after 180s")

    # mssql-cdc has no init hook, so the `rivet` database must be created by hand
    # before anything can enable CDC on it.
    say("ensuring the mssql-cdc 'rivet' database exists")
    ms_sql("IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;", db=None)

    # Wait for fake-gcs (4443) so a following `scenarios` run does not race the
    # CDC→GCS test against a store that is not listening yet.
    if shell.wait_until(lambda: shell.tcp_open("127.0.0.1", 4443), tries=20, delay=1.0):
        say("fake-gcs reachable on 4443")
    else:
        shell.warn("fake-gcs not reachable on 4443 (CDC→cloud scenarios will be skipped)")

    return verify()


def verify() -> int:
    """Re-check that each engine is CDC-*ready*, not merely running.

    Each probe is that engine's actual precondition, and none of them is implied
    by a healthy container:
      * PostgreSQL — `wal_level=logical`, without which a logical slot cannot be
        created at all;
      * MySQL — `binlog_format=ROW` (STATEMENT binlog carries no per-row images),
        reachable as the `rivet` user, which is what the REPLICATION grant hangs
        off;
      * SQL Server — the `rivet` DB AND a running SQL Server Agent: the capture
        job IS the Agent, so with it stopped the change tables simply stay empty;
      * MongoDB — a replica-set PRIMARY: a change stream cannot open against a
        standalone mongod.
    """
    all_ready = True

    pg = pg_sql("SHOW wal_level").stdout.strip()
    if pg == "logical":
        shell.ok("postgres-cdc: wal_level=logical OK")
    else:
        shell.bad(f"postgres-cdc: wal_level='{pg}' (need logical)")
        all_ready = False

    my = my_sql("SELECT @@binlog_format", db=None).stdout.strip()
    if my == "ROW":
        shell.ok("mysql-cdc: binlog_format=ROW OK")
    else:
        shell.bad(f"mysql-cdc: binlog_format='{my}' (need ROW)")
        all_ready = False

    ms = squash(
        ms_sql(
            "SELECT CASE WHEN DB_ID('rivet') IS NULL THEN 'no-db' "
            "WHEN (SELECT status_desc FROM sys.dm_server_services "
            "WHERE servicename LIKE 'SQL Server Agent%')='Running' THEN 'ready' "
            "ELSE 'agent-down' END",
            db=None,
            nocount=True,
        ).stdout
    )
    if ms == "ready":
        shell.ok("mssql-cdc: rivet DB + Agent running OK")
    else:
        shell.bad(f"mssql-cdc: '{ms}' (need rivet DB + Agent)")
        all_ready = False

    mo = squash(mo_sql("db.isMaster().ismaster", db=None).stdout)
    if mo == "true":
        shell.ok("mongo-rs: replica-set primary OK")
    else:
        shell.bad(f"mongo-rs: primary='{mo}' (need true)")
        all_ready = False

    if not all_ready:
        raise shell.Fail("verify: one or more engines not CDC-ready")
    say("verify: ALL FOUR ENGINES CDC-READY")
    return 0


def scenarios() -> int:
    say("building + running the live_cdc scenario suite (serial — shared DBs race in parallel)")
    return shell.stream(
        [
            "cargo", "test", "--test", "live_suite", "--",
            "--ignored", "--test-threads=1",
            "live_cdc::", "live_cdc_mssql::", "live_cdc_mongo::",
        ],
        cwd=ROOT,
        timeout=None,
    ).returncode


def standby() -> int:
    """Primary + streaming replica, then the one scenario that asserts a bounded
    CDC run against a STANDBY fails loudly (a standby cannot create a slot, and
    quietly capturing nothing there would be a silent-loss path)."""
    say("compose --profile cdc-standby up -d (primary + streaming replica)")
    if not dc(
        "--profile", "cdc-standby", "up", "-d",
        "pg-cdc-primary", "pg-cdc-standby",
        show=True, timeout=None,
    ).ok:
        raise shell.Fail("standby compose up failed")

    say("waiting for the standby to reach recovery (pg_is_in_recovery = t)...")

    def in_recovery() -> bool:
        out = pg_sql("SELECT pg_is_in_recovery()", container=STANDBY_C, timeout=30).stdout
        return any(line.strip() == "t" for line in out.splitlines())

    if shell.wait_until(in_recovery, tries=40, delay=3.0):
        say("standby in recovery on :5436")
    else:
        shell.warn("standby did not reach recovery within 120s")

    say("running the standby fail-loud scenario")
    return shell.stream(
        [
            "cargo", "test", "--test", "live_suite", "--",
            "--ignored", "--exact",
            "live_cdc::roast_pg_cdc_bounded_on_a_standby_fails_loud",
        ],
        cwd=ROOT,
        timeout=None,
    ).returncode


def harm(args: Sequence[str] = ()) -> int:
    """Source-side HARM under load (writer p50/p99 with vs without a looping CDC
    drain, plus peak replication lag / slot WAL retention / catalog_xmin age on
    PG). Still a delegation: `dev/cdc/harm.py` is already Python."""
    bin_path = resolve_bin()
    return shell.stream(
        [sys.executable, str(ROOT / "dev/cdc/harm.py"), *args],
        cwd=ROOT,
        env={"RIVET_BIN": str(bin_path)},
        timeout=None,
    ).returncode


def soak_interval(engine: str | None = None, phases: Sequence[str] = ()) -> int:
    """The stand's `soak` subcommand: the drain-INTERVAL memory soak, one engine
    per invocation. Delegates to `dev/cdc_interval/soak_all.sh`, which is a
    different harness from `scripts/soak_cdc.sh` (ported in `cdc_soak.py`) — that
    one is the cycle/leak runner, this one grows the drain interval 12× and
    asserts peak RSS stays flat.

    Still a delegation to the bash, exactly as `stand.sh` did. `dev/pytools/
    cdc_interval.py` is the port of that family; point this at its `soak-all`
    subcommand once it is complete, rather than duplicating the harness here.
    """
    if not engine:
        raise shell.Fail("usage: cdc_stand.py soak <pg|mysql|mssql|mongo> [phases]")
    # The ported harness, as this function's own docstring asked for once
    # `cdc_interval` landed. Delegating to the shell kept its bugs live: an
    # unknown engine ran the whole multi-hour soak creating nothing, and a
    # `rss=0` from an unparsed `/usr/bin/time` made the leak gate `0 -gt 0`,
    # i.e. unfailable — which on Linux was EVERY run.
    from . import cdc_interval

    return cdc_interval.soak_all(
        engine,
        phases=" ".join(phases) if phases else None,
        rivet=str(resolve_bin()),
    )


def down() -> int:
    say(f"compose --profile cdc stop ({' '.join(CDC_SVCS)}); --profile cdc-standby stop")
    dc("--profile", "cdc", "stop", *CDC_SVCS, show=True, timeout=None)
    # The standby profile may never have been started; its absence is not an error.
    dc("--profile", "cdc-standby", "stop", "pg-cdc-primary", "pg-cdc-standby", timeout=None)
    return 0


# ══ perf.sh ════════════════════════════════════════════════════════════════════
PAD = "x" * 48  # a 48-byte payload column, so a row is wide enough to be honest


def _cfg_text(engine: str, ckpt: Path, out: Path) -> str:
    """One bounded (`until_current: true`) `mode: cdc` export named `pf`.

    `until_current` is what makes this a MEASURABLE unit of work: the run stops
    at the backlog that existed when it opened instead of tailing forever.
    """
    if engine == "pg":
        return (
            'source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5434/rivet" }\n'
            "exports:\n"
            "  - { name: pf, table: pf, mode: cdc, format: parquet, "
            f'cdc: {{ checkpoint: "{ckpt}", slot: perf_pg, until_current: true }}, '
            f'destination: {{ type: local, path: "{out}" }} }}\n'
        )
    if engine == "mysql":
        # A server_id distinct from every other harness (soak uses 47001/49333,
        # the parity sweep 47523): two replicas sharing an id kick each other off
        # the binlog stream.
        return (
            'source: { type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }\n'
            "exports:\n"
            "  - { name: pf, table: pf, mode: cdc, format: parquet, "
            f'cdc: {{ checkpoint: "{ckpt}", server_id: 9911, until_current: true }}, '
            f'destination: {{ type: local, path: "{out}" }} }}\n'
        )
    if engine == "mssql":
        return (
            "source:\n"
            "  type: mssql\n"
            f'  url: "sqlserver://sa:{SA_PASSWORD}@127.0.0.1:1434/rivet"\n'
            "  tls: { accept_invalid_certs: true }\n"
            "exports:\n"
            "  - { name: pf, table: pf, mode: cdc, format: parquet, "
            f'cdc: {{ checkpoint: "{ckpt}", capture_instance: dbo_pf, until_current: true }}, '
            f'destination: {{ type: local, path: "{out}" }} }}\n'
        )
    if engine == "mongo":
        return (
            "source: { type: mongo, url: "
            '"mongodb://127.0.0.1:27018/perfdb?directConnection=true&serverSelectionTimeoutMS=5000" }\n'
            "exports:\n"
            "  - { name: pf, table: pf, mode: cdc, format: parquet, "
            f'cdc: {{ checkpoint: "{ckpt}", until_current: true }}, '
            f'destination: {{ type: local, path: "{out}" }} }}\n'
        )
    raise shell.Fail(
        f"unknown engine '{engine}' (expected one of: {', '.join(PERF_ENGINES)})"
    )


def write_cfg(engine: str, work: Path) -> Path:
    """Write `$WORK/<engine>.yaml` and clear that engine's output + checkpoint.

    Clearing them here is what makes the anchor run meaningful: a stale
    checkpoint would resume mid-backlog and a stale output dir would be counted
    as drained rows.
    """
    cfg, ckpt, out = work / f"{engine}.yaml", work / f"{engine}.ckpt", work / f"{engine}.out"
    shell.rm_rf(out)
    shell.rm_rf(ckpt)
    out.mkdir(parents=True, exist_ok=True)
    text = _cfg_text(engine, ckpt, out)  # validates the engine name before writing
    shell.atomic_write(cfg, text)
    return cfg


def setup_engine(engine: str, work: Path, rivet: Path) -> Path:
    """Create the captured table/collection, then PIN the resume anchor.

    The pin is a drain of zero changes. It matters most on MySQL, which has no
    server-side anchor: without a checkpointed open, the next run re-anchors to
    the CURRENT binlog position and the seeded backlog is skipped entirely — the
    engine would report a suspiciously fast 0-row drain. PostgreSQL pins
    server-side at slot creation and SQL Server floors at `fn_cdc_get_min_lsn`,
    so for them the pin is belt-and-suspenders.
    """
    if engine == "pg":
        pg_sql(
            "SELECT pg_drop_replication_slot('perf_pg') FROM pg_replication_slots "
            "WHERE slot_name='perf_pg'"
        )
        pg_sql("DROP TABLE IF EXISTS pf; CREATE TABLE pf(id bigint primary key, v int, pad varchar(64))")
    elif engine == "mysql":
        my_sql("DROP TABLE IF EXISTS pf; CREATE TABLE pf(id bigint primary key, v int, pad varchar(64))")
    elif engine == "mssql":
        ms_sql(
            "IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_pf') "
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='pf',"
            "@capture_instance='dbo_pf';"
        )
        ms_sql(
            "IF OBJECT_ID('dbo.pf') IS NOT NULL DROP TABLE dbo.pf; "
            "CREATE TABLE dbo.pf(id bigint primary key, v int, pad varchar(64));"
        )
        ms_sql(
            "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 "
            "EXEC sys.sp_cdc_enable_db;"
        )
        ms_sql(
            "EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='pf',"
            "@role_name=NULL,@capture_instance='dbo_pf',@supports_net_changes=0;"
        )
    elif engine == "mongo":
        mo_sql("db.pf.drop()")
    else:
        raise shell.Fail(
            f"unknown engine '{engine}' (expected one of: {', '.join(PERF_ENGINES)})"
        )

    cfg = write_cfg(engine, work)
    # The anchor run is allowed to fail (nothing to capture yet on some engines);
    # what it must not do is abort the matrix.
    shell.run([str(rivet), "run", "-c", str(cfg)], cwd=ROOT, timeout=None)
    return cfg


def seed(engine: str, n: int) -> None:
    """Seed the backlog with a single server-side statement per engine — driving
    N inserts from the client would measure the harness, not the capture."""
    if engine == "pg":
        pg_sql(
            f"INSERT INTO pf SELECT g, g%1000, '{PAD}' FROM generate_series(1,{n}) g",
            timeout=None,
        )
    elif engine == "mysql":
        my_sql(
            f"SET SESSION cte_max_recursion_depth=100000000; INSERT INTO pf "
            f"SELECT g, g%1000, '{PAD}' FROM (WITH RECURSIVE s(g) AS "
            f"(SELECT 1 UNION ALL SELECT g+1 FROM s WHERE g<{n}) SELECT g FROM s) q",
            timeout=None,
        )
    elif engine == "mssql":
        ms_sql(
            f"WITH n AS (SELECT 1 g UNION ALL SELECT g+1 FROM n WHERE g<{n}) "
            f"INSERT INTO dbo.pf SELECT g, g%1000, '{PAD}' FROM n OPTION (MAXRECURSION 0);",
            nocount=True,
            timeout=None,
        )
    elif engine == "mongo":
        mo_sql(
            f"var d=[];for(var i=1;i<={n};i++){{d.push({{_id:i,v:i%1000,pad:'{PAD}'}});"
            "if(d.length===5000){db.pf.insertMany(d);d.length=0;}}"
            "if(d.length)db.pf.insertMany(d)",
            timeout=None,
        )


def mssql_catchup(n: int, *, tries: int = 120, delay: float = 2.0) -> bool:
    """Wait for the Agent's capture job to move N rows into `cdc.dbo_pf_CT`.

    SQL Server CDC is ASYNCHRONOUS: the insert returning says nothing about the
    change table. Draining before the Agent has caught up measures a partial
    backlog and reports it as throughput.
    """

    def caught_up() -> bool:
        got = first_int(ms_sql("SELECT COUNT_BIG(*) FROM cdc.dbo_pf_CT", nocount=True).stdout)
        return got is not None and got >= n

    return shell.wait_until(caught_up, tries=tries, delay=delay)


def drained_rows(engine: str, work: Path) -> int:
    """Distinct captured ids in the drained parquet, per an INDEPENDENT reader."""
    out = work / f"{engine}.out"
    parts = sorted(out.glob("cdc-*.parquet"))
    if not parts:
        return 0
    key = '"_id"' if engine == "mongo" else "id"
    value = duckdb_scalar(
        f"SELECT COUNT(DISTINCT {key}) FROM read_parquet('{out}/cdc-*.parquet') "
        "WHERE __op='insert'"
    )
    if value is None:
        # Parts exist but could not be read: say so instead of reporting 0 rows,
        # which is indistinguishable from a drain that captured nothing.
        shell.bad(f"{engine}: {len(parts)} part(s) written but duckdb could not read them")
        return 0
    return int(value)


def cleanup_engine(engine: str) -> None:
    """Drop what the run created. This is `finally`-scoped for a reason: a leaked
    `perf_pg` slot keeps pinning WAL on the stand forever, and a leaked `dbo_pf`
    capture instance makes the next run's `sp_cdc_enable_table` fail."""
    if engine == "pg":
        pg_sql(
            "SELECT pg_drop_replication_slot('perf_pg') FROM pg_replication_slots "
            "WHERE slot_name='perf_pg'"
        )
        pg_sql("DROP TABLE IF EXISTS pf")
    elif engine == "mysql":
        my_sql("DROP TABLE IF EXISTS pf")
    elif engine == "mssql":
        ms_sql(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='pf',"
            "@capture_instance='dbo_pf'; DROP TABLE IF EXISTS dbo.pf;"
        )
    elif engine == "mongo":
        mo_sql("db.pf.drop()")


ROW_FMT = "{:<10} {:<9} {:<8} {:<10} {:<8}"


def perf(
    engines: Sequence[str] = (),
    *,
    n: int | None = None,
    work: str | Path | None = None,
    binary: str | Path | None = None,
) -> int:
    """The bulk-backlog capture-throughput matrix.

    One drain of a whole N-row backlog per engine at the default-ish rollover.
    This is NOT the steady-state memory contract — for RSS flatness as the
    backlog grows use `cdc_stand.py soak <engine>` (dev/cdc_interval/soak_all.sh).
    """
    engine_list = list(engines) or list(PERF_ENGINES)
    unknown = [e for e in engine_list if e not in PERF_ENGINES]
    if unknown:
        # The bash `case` had no default arm, so an unknown name set nothing up
        # and produced a row of zeros indistinguishable from a broken engine.
        raise shell.Fail(
            f"unknown engine(s): {', '.join(unknown)} "
            f"(expected any of: {', '.join(PERF_ENGINES)})"
        )

    rivet = resolve_bin(binary)
    rows = int(n if n is not None else os.environ.get("N") or 100000)
    work_dir = Path(work or os.environ.get("WORK") or "/tmp/rivet-cdc-perf")
    shell.require(
        "duckdb",
        hint="the drained-row count needs an independent reader; without it the "
             "matrix cannot tell a fast drain from an empty one",
    )
    shell.require("/usr/bin/time", hint="install GNU time (Debian/Ubuntu: apt install time)")

    shell.rm_rf(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    table: list[str] = []
    for e in engine_list:
        shell.log("setup + anchor...", tag=e)
        cfg = setup_engine(e, work_dir, rivet)
        try:
            shell.log(f"seeding {rows} rows...", tag=e)
            seed(e, rows)
            if e == "mssql":
                shell.log("waiting for Agent capture...", tag=e)
                if not mssql_catchup(rows):
                    shell.warn(
                        "mssql: the Agent did not extract "
                        f"{rows} rows into cdc.dbo_pf_CT within 240s — "
                        "the drain below measures a PARTIAL backlog"
                    )
            shell.log("timed bounded drain...", tag=e)
            timed = timed_run([str(rivet), "run", "-c", str(cfg)], work_dir / f"{e}.time")
            if not timed.proc.ok:
                # Same as the bash `|| cat "$WORK/$e.time"`: the report is the
                # only diagnosis left, so it goes to the terminal.
                print(timed.proc.stderr, file=sys.stderr)
            drained = drained_rows(e, work_dir)
            rps = drained / (timed.seconds + 0.0001)
            if timed.rss_bytes is None:
                shell.warn(f"{e}: no max-RSS line in the /usr/bin/time report")
            rss_mb = (timed.rss_bytes or 0) // 1048576
            table.append(
                ROW_FMT.format(e, str(drained), f"{timed.seconds:.2f}s", f"{rps:.0f}", f"{rss_mb}MB")
            )
        finally:
            cleanup_engine(e)

    print()
    print(f"=== CDC capture-throughput (bulk backlog of {rows} rows, {rivet.name}) ===")
    print(ROW_FMT.format("engine", "rows", "wall", "rows/s", "peakRSS"))
    for row in table:
        print(row)
    return 0


# ══ CLI ════════════════════════════════════════════════════════════════════════
def _parse_perf_args(args: Sequence[str]) -> tuple[list[str], dict[str, object]]:
    """Positional engines plus the three knobs the bash took only as environment
    variables (`N`, `WORK`, `RIVET_BIN`). The env forms still work; a flag wins."""
    engines: list[str] = []
    kw: dict[str, object] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--n", "-n") and i + 1 < len(args):
            kw["n"] = int(args[i + 1]); i += 2
        elif a == "--work" and i + 1 < len(args):
            kw["work"] = args[i + 1]; i += 2
        elif a == "--bin" and i + 1 < len(args):
            kw["binary"] = args[i + 1]; i += 2
        elif a.startswith("-"):
            raise shell.Fail(f"perf: unknown option {a}", hint=USAGE)
        else:
            engines.append(a); i += 1
    return engines, kw


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else ""
    rest = args[1:]

    if cmd == "up":
        return up()
    if cmd == "verify":
        return verify()
    if cmd == "scenarios":
        return scenarios()
    if cmd == "standby":
        return standby()
    if cmd == "perf":
        engines, kw = _parse_perf_args(rest)
        return perf(engines, **kw)  # type: ignore[arg-type]
    if cmd == "harm":
        return harm(rest)
    if cmd == "soak":
        return soak_interval(rest[0] if rest else None, rest[1:])
    if cmd == "all":
        # `up && scenarios && perf`: stop at the first failure, keep its code.
        for step in (up, scenarios, lambda: perf()):
            code = step()
            if code:
                return code
        return 0
    if cmd == "down":
        return down()

    print(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
