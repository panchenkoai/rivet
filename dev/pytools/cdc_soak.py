"""CDC soak runner + CDC source-parity sweep.

Port of two long-running verifiers over the same `cdc` compose profile:

* `scripts/soak_cdc.sh`          → `soak()`
  A scheduler-shaped loop of bounded CDC runs against continuous churn. It is
  the leak/degradation detector no single-shot test can be, and it asserts three
  things at the end: every cycle succeeded, every churned id is present in the
  union of the parts (no gap), and the peak RSS of the LAST quarter of cycles is
  within 1.5× of the FIRST quarter's (a real leak grows every cycle and blows
  well past that; steady-state noise does not).

* `dev/sweep/source_parity_cdc.sh` → `source_parity()`
  An INDEPENDENT value-parity sweep through the CDC path (PG logical slot /
  MySQL binlog / SQL Server capture job). The CDC parquet is deduped to
  current-state — latest change per id by `__pos`,`__seq`, deletes excluded —
  and then compared against the source per column. This is a different
  value-decode path from batch (each engine has its own `build_column`), and it
  is where the uuid→NULL cell bug lived: 100% of a column silently became NULL
  while every count/sum check passed.

Usage:

    python3 dev/pytools/cdc_soak.py soak [minutes] [rivet-binary]
    python3 dev/pytools/cdc_soak.py source-parity

Both share `cdc_stand.py`'s stand plumbing (the container SQL wrappers, the
rivet-binary resolution and — importantly — the single `/usr/bin/time` parser).

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH:

1. Teardown is `finally`-scoped in both. The soak's bash `trap … EXIT` did cover
   its tempdir and table, but the parity sweep had NO trap: any early exit (the
   two exit-2 preconditions, or a Ctrl-C mid-sweep) leaked `sweep_src`, the
   `sweep_slot` replication slot — which then pins WAL on the stand
   indefinitely — the `dbo_sweep_src` capture instance, and the tempdir.
2. `soak` checks for `duckdb` and for `binlog_format=ROW` BEFORE it starts, not
   after ten minutes of churn. Both were end-of-run failures in the bash: the
   duckdb one by design (its own comment insists a missing reader must be loud),
   the ROW one not at all — with STATEMENT binlog the capture is empty and the
   soak spent its whole runtime earning a gap failure it could have predicted in
   one query.
3. Zero RSS samples is now a named failure. The bash ran `wc -l < "$RSS_LOG"` on
   a file that is only created when at least one sample parsed, so the failure
   mode was `set -e` aborting on a shell redirect error — no diagnosis at all.
4. RSS samples are normalised to BYTES by the shared parser (BSD `-l` reports
   bytes, GNU `-v` kilobytes). The 1.5× trend ratio is unaffected — it always
   compared like with like — but the printed numbers are now the same unit on
   both platforms.
5. The pin run's failure is loud, with the MySQL CDC prerequisites named. Under
   `set -e` the bash aborted on it with its output redirected to /dev/null, i.e.
   a silent exit 1.
6. A `SHOW MASTER STATUS` that returns nothing (binary logging off, or MySQL
   ≥8.4 where it is renamed) is a `Fail` in the parity sweep. The bash wrote
   `{"file":"","pos":}` — invalid JSON — into the checkpoint and reported the
   resulting failure as a generic "rivet FAILED".

KNOWN-WEAK ORACLE, kept as-is for fidelity: the parity sweep's `norm()` strips
trailing zeros (`sed 's/0*$//'`) so the two sides can agree across engines that
render decimals differently. It also means `1000` normalises to `1`, so the
comparison is blind to some magnitude differences in the LAST field of a value.
Reproduced exactly rather than tightened, because tightening it would change
which cells fail; flagged here so the next reader knows the check is weaker than
it looks.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

if __package__:
    from . import cdc_stand, shell
else:  # executed as a plain script: `python3 dev/pytools/cdc_soak.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cdc_stand  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT

USAGE = (
    "usage: dev/pytools/cdc_soak.py soak [minutes] [rivet-binary]\n"
    "       dev/pytools/cdc_soak.py source-parity"
)


# ══ scripts/soak_cdc.sh ════════════════════════════════════════════════════════
SOAK_TABLE = "soak_cdc"
# Churn per cycle: 200 inserts, then updates on every 7th and deletes on every
# 31st of the ids this cycle introduced. Updates and deletes are what make the
# gap check meaningful — an insert-only stream never exercises the dedup or the
# tombstone path.
SOAK_INSERTS = 200
SOAK_CFG = """source: {{ type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }}
exports:
  - name: soak_cdc
    table: soak_cdc
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: 47001, rollover: 500 }}
    destination: {{ type: local, path: "{out}" }}
"""


def _soak_preflight() -> None:
    """Everything that can be known in one query, asked before the clock starts.

    `binlog_format` is MySQL's CDC prerequisite: STATEMENT/MIXED binlog carries
    no per-row images, so the capture would be empty and the gap assertion would
    fail — correctly, but ten minutes later.
    """
    if not shell.have("duckdb"):
        # The bash's own wording, moved to the FRONT of the run: it insisted a
        # missing reader be loud, then only found out after the last cycle.
        raise shell.Fail(
            "soak: duckdb missing — cannot verify gap-freedom",
            hint="brew install duckdb / apt install duckdb",
        )
    shell.require("/usr/bin/time", hint="install GNU time (Debian/Ubuntu: apt install time)")
    fmt = cdc_stand.my_sql("SELECT @@binlog_format", db=None).stdout.strip()
    if fmt != "ROW":
        raise shell.Fail(
            f"mysql-cdc: binlog_format='{fmt}' (need ROW)",
            hint="dev/pytools/cdc_stand.py up  — and check the container is rivet-mysql-cdc-1",
        )


def _churn(next_id: int) -> int:
    """One cycle of churn; returns the next unused id.

    The tuple list is built in Python because macOS `seq` rejects a two-
    conversion format string — the bash reached for `awk` to work around it.
    """
    end_id = next_id + SOAK_INSERTS - 1
    values = ",".join(f"({i},{i})" for i in range(next_id, end_id + 1))
    cdc_stand.my_sql(f"INSERT INTO {SOAK_TABLE} VALUES {values}").check("soak: INSERT")
    cdc_stand.my_sql(
        f"UPDATE {SOAK_TABLE} SET v = v + 1 WHERE id % 7 = 0 AND id >= {next_id}"
    ).check("soak: UPDATE")
    cdc_stand.my_sql(
        f"DELETE FROM {SOAK_TABLE} WHERE id % 31 = 0 AND id >= {next_id}"
    ).check("soak: DELETE")
    return end_id + 1


def _tail(path: Path, lines: int) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return ""


def soak(minutes: int | float = 10, binary: str | Path | None = None) -> int:
    """Run bounded CDC cycles for `minutes`, then assert cycles/gap/RSS-trend."""
    mins = int(minutes)
    rivet = cdc_stand.resolve_bin(binary)
    _soak_preflight()

    work = Path(tempfile.mkdtemp(prefix="rivet-soak-cdc-"))
    out = work / "out"
    ckpt = work / "cdc.ckpt"
    run_log = work / "run.log"
    time_log = work / "time.log"
    out.mkdir(parents=True, exist_ok=True)

    try:
        cdc_stand.my_sql(
            f"DROP TABLE IF EXISTS {SOAK_TABLE}; "
            f"CREATE TABLE {SOAK_TABLE} (id BIGINT PRIMARY KEY, v BIGINT)"
        ).check("soak: CREATE TABLE")

        cfg = work / "cfg.yaml"
        shell.atomic_write(cfg, SOAK_CFG.format(ckpt=ckpt, out=out))

        # PIN FIRST. MySQL has no server-side anchor: the checkpoint pins at the
        # CURRENT binlog coordinates on its first open, so any churn issued
        # before the pin is invisible by the (tested) anchor model — and would
        # read as a gap that is really a harness bug.
        pin = shell.run([str(rivet), "run", "--config", str(cfg)], cwd=ROOT, timeout=None)
        if not pin.ok:
            raise shell.Fail(
                "soak: the anchor (pin) run failed — refusing to soak against an unpinned stream",
                hint="MySQL CDC needs ROW binlog, a REPLICATION SLAVE/CLIENT grant for "
                     "'rivet', and a server_id nobody else is using (this harness takes 47001)",
            )

        deadline = time.time() + mins * 60
        cycle = 0
        next_id = 1
        rss_samples: list[int] = []
        print(f"soak: {mins}m, bin={rivet}")

        while time.time() < deadline:
            cycle += 1
            next_id = _churn(next_id)
            timed = cdc_stand.timed_run(
                [str(rivet), "run", "--config", str(cfg)], time_log
            )
            run_log.write_text(timed.proc.stdout)
            if not timed.proc.ok:
                print(_tail(run_log, 30), file=sys.stderr)
                print(_tail(time_log, 5), file=sys.stderr)
                raise shell.Fail(f"soak: cycle {cycle} FAILED")
            if timed.rss_bytes is not None:
                rss_samples.append(timed.rss_bytes)
            time.sleep(2)

        # ── assertions ────────────────────────────────────────────────────────
        # An INDEPENDENT reader over the parts: counting with rivet's own
        # manifest would make the gap check a self-oracle.
        seen = cdc_stand.duckdb_scalar(
            f"SELECT COUNT(DISTINCT id) FROM read_parquet('{out}/*.parquet')"
        )
        expected = next_id - 1
        print(
            f"soak: cycles={cycle}, churned_ids={expected}, "
            f"distinct_in_parts={seen if seen is not None else -1}"
        )
        if seen is None:
            # A missing/failed reader must be LOUD: a silently skipped gap check
            # reads as "verified" when nothing was.
            raise shell.Fail("soak: could not read the parts — cannot verify gap-freedom")
        rows_seen = int(seen)
        if rows_seen < expected:
            raise shell.Fail(f"soak: GAP — {rows_seen} < {expected}")

        if not rss_samples:
            raise shell.Fail(
                "soak: no RSS samples — cannot verify the memory trend",
                hint=f"/usr/bin/time {cdc_stand.time_flag()} produced no max-RSS line; "
                     f"see {time_log}",
            )
        quarter = max(1, len(rss_samples) // 4)
        first = max(rss_samples[:quarter])
        last = max(rss_samples[-quarter:])
        print(f"soak: peak RSS first-quarter={first} last-quarter={last}")
        # 1.5× budget, integer-truncated exactly as the bash computed it.
        if last > first * 3 // 2:
            raise shell.Fail("soak: RSS TREND FAIL — last-quarter peak > 1.5× first-quarter")

        print("soak: OK")
        return 0
    finally:
        # The bash trap's two jobs. They run on the failure paths too, so a
        # failed soak does not leave `soak_cdc` behind for the next one to trip
        # over (a stale table makes the next run's CREATE fail, and its binlog
        # events would be attributed to the new run).
        shutil.rmtree(work, ignore_errors=True)
        cdc_stand.my_sql(f"DROP TABLE IF EXISTS {SOAK_TABLE}")


# ══ dev/sweep/source_parity_cdc.sh ══════════════════════════════════════════════
SWEEP_ROWS = 1000
COLS: tuple[str, ...] = ("id", "k", "maybe_null", "amount", "label", "uid", "ts", "payload", "d")
# Only these get a SUM check on top of the count/non-null/distinct triple: a sum
# catches a value that decoded to the wrong NUMBER, which counts cannot see.
NUMS: frozenset[str] = frozenset({"id", "k", "maybe_null", "amount"})


@dataclass
class Tally:
    total: int = 0
    match: int = 0
    failed: int = 0


def norm(value: str) -> str:
    """`tr -d '[:space:]'` then `sed 's/0*$//;s/\\.$//'`.

    Kept bug-for-bug — see the module docstring: this also strips trailing zeros
    from integers, so the last field of a value is compared loosely.
    """
    squashed = "".join(value.split())
    squashed = re.sub(r"0+$", "", squashed)
    return re.sub(r"\.$", "", squashed)


def chk(t: Tally, name: str, src: str, dest: str) -> None:
    t.total += 1
    if norm(src) == norm(dest):
        t.match += 1
        print(f"  {name:<14} {src:<22} ok")
    else:
        t.failed += 1
        print(f"  {name:<14} src={src} dest={dest}  *** MISMATCH ***")


def ddv(query: str) -> str:
    """duckdb in `-list` mode. An empty string on failure, which then shows up as
    a MISMATCH — the sweep must never treat "could not read the destination" as
    agreement."""
    p = shell.run(["duckdb", "-noheader", "-list", "-c", query], cwd=ROOT, timeout=None)
    return p.stdout.strip() if p.ok else ""


def view(glob: str) -> str:
    """Dedup-to-current-state over a CDC parquet glob.

    The CDC parquet is a CHANGE LOG, so comparing it to the source directly
    would fail on every updated row. Latest change per id by (`__pos`,`__seq`),
    deletes excluded, is the current-state projection the source can be compared
    against.
    """
    return (
        "(SELECT * FROM (SELECT *, row_number() OVER "
        f"(PARTITION BY id ORDER BY __pos DESC,__seq DESC) rn FROM read_parquet('{glob}')) "
        "WHERE rn=1 AND __op <> 'delete')"
    )


def compare(
    t: Tally, stat: Callable[[str], str], total: Callable[[str], str], v: str
) -> None:
    """Per column: count/non-null/distinct on both sides, plus SUM for numerics.

    The per-column null profile is the load-bearing part. A "degrade to NULL"
    cell path (the FixedSizeBinary(16) uuid builder) turned 100% of a column into
    NULLs while hand-picked count/sum checks stayed green — only a per-column
    non-null + distinct count against the source catches that.
    """
    for col in COLS:
        chk(
            t,
            col,
            stat(col),
            ddv(f"SELECT count(*)||'/'||count({col})||'/'||count(distinct {col}) FROM {v}"),
        )
        if col in NUMS:
            chk(
                t,
                f"{col}/sum",
                total(col),
                ddv(f"SELECT COALESCE(SUM({col}),0)::VARCHAR FROM {v}"),
            )


def dc_exec(service: str, *argv: str, timeout: float | None = 600) -> shell.Proc:
    """`docker compose exec -T <service> …` from the repo root. The sweep talks to
    the compose SERVICE rather than the container name, so it follows a renamed
    project."""
    return cdc_stand.dc("exec", "-T", service, *argv, timeout=timeout)


def cdc_run(t: Tally, rivet: Path, engine: str, url: str, cdc_opts: str, out: Path) -> None:
    """Write `<out>.yaml` and run one bounded CDC capture into `<out>`."""
    cfg = Path(f"{out}.yaml")
    shell.atomic_write(
        cfg,
        f'source: {{ type: {engine}, url: "{url}" }}\n'
        "exports:\n"
        "  - name: s\n"
        "    table: sweep_src\n"
        "    mode: cdc\n"
        "    format: parquet\n"
        f"    cdc: {{ {cdc_opts} }}\n"
        f'    destination: {{ type: local, path: "{out}" }}\n',
    )
    if not shell.run([str(rivet), "run", "--config", str(cfg)], cwd=ROOT, timeout=None).ok:
        print("  rivet FAILED")
        t.failed += 1


def _sweep_postgres(t: Tally, rivet: Path, out: Path) -> None:
    def pg(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return dc_exec("postgres-cdc", *cdc_stand.psql_argv(q), timeout=timeout)

    print("== CDC postgres ==")
    pg(
        "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
        "k INT, maybe_null INT, amount DECIMAL(38,10), label TEXT, uid UUID, ts TIMESTAMP, "
        "payload JSONB, d DATE);"
    ).check("postgres: CREATE TABLE sweep_src")
    pg(
        "SELECT pg_drop_replication_slot('sweep_slot') FROM pg_replication_slots "
        "WHERE slot_name='sweep_slot';"
    )
    try:
        # `test_decoding` is the TEXT-rendering reader — the one whose parser has
        # to survive the session's timezone/datestyle/bytea_output. Creating the
        # slot is also the wal_level=logical assertion: it cannot succeed without.
        pg("SELECT pg_create_logical_replication_slot('sweep_slot','test_decoding');").check(
            "postgres: create logical slot (needs wal_level=logical)"
        )
        pg(
            "INSERT INTO sweep_src SELECT g,g%50,CASE WHEN g%3=0 THEN NULL ELSE g END,"
            "(g::numeric*0.0000000001),CASE WHEN g%7=0 THEN NULL ELSE 'rôw_😀_'||(g%100) END,"
            "gen_random_uuid(),timestamp '2020-01-01'+(g||' minutes')::interval,"
            f"json_build_object('k',g%50,'v',g)::jsonb,date '2020-01-01'+g "
            f"FROM generate_series(1,{SWEEP_ROWS}) g;",
            timeout=None,
        ).check("postgres: seed sweep_src")

        cdc_run(
            t,
            rivet,
            "postgres",
            "postgresql://rivet:rivet@127.0.0.1:5434/rivet",
            "slot: sweep_slot, until_current: true",
            out / "pg",
        )
        compare(
            t,
            lambda c: pg(
                f"SELECT count(*)||'/'||count({c})||'/'||count(distinct {c}) FROM sweep_src"
            ).stdout.strip(),
            lambda c: pg(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
            view(str(out / "pg" / "*.parquet")),
        )
    finally:
        # An orphaned slot keeps pinning WAL on the stand until someone notices
        # the disk — the bash left one behind on every early exit.
        pg(
            "SELECT pg_drop_replication_slot('sweep_slot') FROM pg_replication_slots "
            "WHERE slot_name='sweep_slot';"
        )
        pg("DROP TABLE IF EXISTS sweep_src;")


def _sweep_mysql(t: Tally, rivet: Path, out: Path) -> None:
    def my(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return dc_exec("mysql-cdc", *cdc_stand.mysql_argv(q), timeout=timeout)

    print("== CDC mysql ==")
    my(
        "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
        "k INT, maybe_null INT, amount DECIMAL(38,10), label VARCHAR(60), uid CHAR(36), "
        "ts DATETIME, payload JSON, d DATE);"
    ).check("mysql: CREATE TABLE sweep_src")
    try:
        # MySQL has no server-side anchor, so the checkpoint is written BY HAND
        # from the current coordinates before the seed: that is the pin, and
        # without it the capture would start after the rows it is meant to read.
        status = my("SHOW MASTER STATUS").stdout
        fields = status.split()
        if len(fields) < 2:
            raise shell.Fail(
                "mysql-cdc: SHOW MASTER STATUS returned nothing — cannot pin the binlog anchor",
                hint="binary logging must be on (log_bin=ON, binlog_format=ROW); on MySQL "
                     "8.4+ the statement is SHOW BINARY LOG STATUS",
            )
        binlog_file, binlog_pos = fields[0], fields[1]
        ckpt = out / "myckpt"
        shell.atomic_write(ckpt, f'{{"file":"{binlog_file}","pos":{binlog_pos}}}\n')

        my(
            "SET SESSION cte_max_recursion_depth=4000; INSERT INTO sweep_src "
            "WITH RECURSIVE seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE "
            f"g<{SWEEP_ROWS}) SELECT g,g%50,IF(g%3=0,NULL,g),CAST(g AS DECIMAL(38,10))"
            "*0.0000000001,IF(g%7=0,NULL,CONCAT('rôw_😀_',g%100)),UUID(),"
            "TIMESTAMP('2020-01-01')+INTERVAL g MINUTE,JSON_OBJECT('k',g%50,'v',g),"
            "DATE('2020-01-01')+INTERVAL g DAY FROM seq;",
            timeout=None,
        ).check("mysql: seed sweep_src")

        cdc_run(
            t,
            rivet,
            "mysql",
            "mysql://rivet:rivet@127.0.0.1:3307/rivet",
            f'until_current: true, checkpoint: "{ckpt}", server_id: 47523',
            out / "my",
        )
        compare(
            t,
            lambda c: my(
                f"SELECT CONCAT(count(*),'/',count({c}),'/',count(distinct {c})) FROM sweep_src"
            ).stdout.strip(),
            lambda c: my(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
            view(str(out / "my" / "*.parquet")),
        )
    finally:
        my("DROP TABLE IF EXISTS sweep_src;")


def _sweep_mssql(t: Tally, rivet: Path, out: Path) -> None:
    def ms(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return dc_exec(
            "mssql-cdc",
            *cdc_stand.sqlcmd_argv(q, wide=True, nocount=True),
            timeout=timeout,
        )

    print("== CDC mssql ==")
    ms(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) "
        "EXEC sys.sp_cdc_enable_db;"
    ).check("mssql: sp_cdc_enable_db")
    ms(
        "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src; "
        "CREATE TABLE dbo.sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, "
        "amount DECIMAL(38,10), label NVARCHAR(60), uid UNIQUEIDENTIFIER, ts DATETIME2, "
        "payload NVARCHAR(200), d DATE);"
    ).check("mssql: CREATE TABLE sweep_src")
    try:
        ms(
            "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'sweep_src', "
            "@role_name=NULL, @capture_instance=N'dbo_sweep_src';"
        ).check("mssql: sp_cdc_enable_table (needs the SQL Server Agent running)")
        ms(
            ";WITH seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE "
            f"g<{SWEEP_ROWS}) INSERT INTO dbo.sweep_src SELECT g,g%50,"
            "CASE WHEN g%3=0 THEN NULL ELSE g END,CAST(g*0.0000000001 AS DECIMAL(38,10)),"
            "CASE WHEN g%7=0 THEN NULL ELSE N'rôw_'+CAST(g%100 AS NVARCHAR(10)) END,NEWID(),"
            "DATEADD(MINUTE,g,CAST('2020-01-01' AS DATETIME2)),"
            "N'{\"k\":'+CAST(g%50 AS NVARCHAR(10))+N',\"v\":'+CAST(g AS NVARCHAR(10))+N'}',"
            "DATEADD(DAY,g,CAST('2020-01-01' AS DATE)) FROM seq OPTION (MAXRECURSION 0);",
            timeout=None,
        ).check("mssql: seed sweep_src")

        # SQL Server CDC is ASYNCHRONOUS — the capture job IS the Agent. Draining
        # before it has moved the rows into cdc.dbo_sweep_src_CT compares the
        # source against a partial capture.
        def captured() -> bool:
            got = cdc_stand.first_int(ms("SELECT COUNT(*) FROM cdc.dbo_sweep_src_CT").stdout)
            return got is not None and got >= SWEEP_ROWS

        if not shell.wait_until(captured, tries=60, delay=0.5):
            # Warn rather than raise: the comparison below still runs, so the
            # per-column mismatches (and the non-zero exit) still happen — but
            # the reader gets told WHY 20 cells are about to disagree.
            shell.warn(
                "mssql: the Agent did not extract "
                f"{SWEEP_ROWS} rows into cdc.dbo_sweep_src_CT within 30s "
                "(is SQL Server Agent running?) — the comparison below is against a "
                "PARTIAL capture"
            )

        cdc_run(
            t,
            rivet,
            "mssql",
            f"sqlserver://sa:{cdc_stand.SA_PASSWORD}@127.0.0.1:1434/rivet",
            f'capture_instance: dbo_sweep_src, checkpoint: "{out / "msckpt"}"',
            out / "ms",
        )
        compare(
            t,
            lambda c: ms(
                f"SELECT CAST(count(*) AS VARCHAR)+'/'+CAST(count({c}) AS VARCHAR)+'/'"
                f"+CAST(count(distinct {c}) AS VARCHAR) FROM dbo.sweep_src"
            ).stdout.strip(),
            lambda c: ms(
                f"SELECT CAST(COALESCE(SUM({c}),0) AS VARCHAR(64)) FROM dbo.sweep_src"
            ).stdout.strip(),
            view(str(out / "ms" / "*.parquet")),
        )
    finally:
        ms(
            "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'sweep_src', "
            "@capture_instance=N'dbo_sweep_src';"
        )
        ms("IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src;")


def source_parity(binary: str | Path | None = None) -> int:
    """The CDC value-parity sweep across PostgreSQL, MySQL and SQL Server.

    All three engines always run: a sweep that quietly drops an engine is worse
    than one that fails, because the missing engine's decode path is exactly
    where a silent-corruption bug survives.
    """
    # `RIVET`, defaulting to the DEBUG build — the sweep is about value fidelity,
    # not speed, and debug is what a developer has just built.
    spelled = str(binary or os.environ.get("RIVET") or "target/debug/rivet")
    rivet = Path(spelled)
    if not rivet.is_absolute():
        rivet = ROOT / rivet
    if not (rivet.is_file() and os.access(rivet, os.X_OK)):
        raise shell.Fail(f"rivet not at {spelled}", code=2)
    if not shell.have("duckdb"):
        raise shell.Fail("duckdb CLI not on PATH", code=2)

    t = Tally()
    out = Path(tempfile.mkdtemp(prefix="rivet-parity-cdc-"))
    print("###### rivet source-parity sweep (CDC) ######")
    try:
        _sweep_postgres(t, rivet, out)
        _sweep_mysql(t, rivet, out)
        _sweep_mssql(t, rivet, out)
    finally:
        shutil.rmtree(out, ignore_errors=True)

    print("#############################################")
    print(f"CDC: {t.match}/{t.total} independent checks matched the source ({t.failed} failed)")
    if t.failed:
        raise shell.Fail("SILENT-CORRUPTION DETECTED")
    return 0


# ══ CLI ════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else ""
    rest = args[1:]

    if cmd == "soak":
        # Positional, as in the bash: [minutes] [rivet-binary].
        minutes = int(rest[0]) if rest else 10
        return soak(minutes, rest[1] if len(rest) > 1 else None)
    if cmd in ("source-parity", "source_parity", "parity"):
        return source_parity(rest[0] if rest else None)

    print(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
