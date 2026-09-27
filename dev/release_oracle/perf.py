"""Performance and resources, this binary against the PREVIOUS release, off the happy path too.

`release_regression` times one PostgreSQL keyset export; `harm_regression` compares what a
batch export costs the source. Neither sees CDC, an incremental delta, a crash resume or
any engine but the one. A 3× slower CDC drain, or a resume that re-reads the table, would
ship through every correctness check.

Per path, each binary runs from its own directory and state: one warm-up, then three
measured runs; the MINIMUM of each metric is compared (other activity only ever adds).

  wall   seconds                  ≤ prev × RIVET_PERF_WALL_TOL (1.5) + 0.1 s
  cpu    user + sys seconds       ≤ prev × RIVET_PERF_CPU_TOL (1.3) + 0.05 s
  rss    peak resident bytes      ≤ prev × RIVET_PERF_RSS_TOL (1.3) + 16 MiB
  harm   the engine's graded      ≤ prev × RIVET_HARM_TOL (1.25) + RIVET_HARM_SLACK (200)
         source counters

Paths: batch `full`, keyset (`chunked`), an incremental delta and a crash→resume per SQL
engine; per CDC engine a drain of a large change set (one big transaction plus many small),
the same drain with the transaction buffer capped so it spills, and a resume after a crash
between flush and ack. Configs for batch come from the previous release's `rivet init`.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .core import Ledger, rivet_bin, run
from .regression import (
    HARM_GRADED,
    _TIME_BIN,
    _is_bsd_time,
    _last_run_harm,
    _parse_rss,
    _parse_wall,
    _require_prev_binary,
    _tolerance,
)
from .upgrade import _declared, _seed, _sql

__all__ = ["verify_perf_regression"]

SCEN = "perf_regression"
ROWS = 200_000
DELTA = 20_000
CDC_CHANGES = 20_000
BIG_ROWS = 2_000_000
REPS = 3
MIB = 1024 * 1024


@dataclass(frozen=True)
class Sample:
    """One measured run."""

    ok: bool
    wall: float
    cpu: float
    rss: int
    harm: dict[str, int]


def _cpu(text: str) -> float:
    """user + sys seconds from either `/usr/bin/time` dialect."""
    m = re.search(r"([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys", text)
    if m:
        return float(m.group(2)) + float(m.group(3))
    u = re.search(r"User time \(seconds\): ([\d.]+)", text)
    s = re.search(r"System time \(seconds\): ([\d.]+)", text)
    return float(u.group(1)) + float(s.group(1)) if u and s else 0.0


def _timed(binary: Path, cwd: Path, env: dict[str, str], *args: str) -> Sample:
    """`binary args…` under `/usr/bin/time` in `cwd`: wall, CPU, peak RSS and the run's harm."""
    flag = "-l" if _is_bsd_time() else "-v"
    p = run([str(_TIME_BIN), flag, str(binary), *args], timeout=None, env=env, cwd=cwd)
    return Sample(p.returncode == 0, _parse_wall(p.stderr)[1], _cpu(p.stderr),
                  _parse_rss(p.stderr), _last_run_harm(cwd))


def _best(samples: list[Sample]) -> Sample | None:
    """The per-metric minimum over successful samples, or None when any run failed."""
    if not samples or not all(s.ok for s in samples):
        return None
    keys = set.intersection(*(set(s.harm) for s in samples)) if samples else set()
    return Sample(True, min(s.wall for s in samples), min(s.cpu for s in samples),
                  min(s.rss for s in samples), {k: min(s.harm[k] for s in samples) for k in keys})


def perf_verdict(engine: str, prev: Sample, cur: Sample) -> list[str]:
    """The metrics on which `cur` regressed past `prev`'s tolerance (empty = none)."""
    wt = _tolerance(os.environ.get("RIVET_PERF_WALL_TOL") or "1.5")
    ct = _tolerance(os.environ.get("RIVET_PERF_CPU_TOL") or "1.3")
    rt = _tolerance(os.environ.get("RIVET_PERF_RSS_TOL") or "1.3")
    ht = _tolerance(os.environ.get("RIVET_HARM_TOL") or "1.25")
    hs = int(os.environ.get("RIVET_HARM_SLACK") or "200")
    worse = []
    # Absolute slack under the ratio: a 0.02 s CPU reading moves by a scheduler tick.
    if cur.wall > prev.wall * wt + 0.1:
        worse.append(f"wall {cur.wall:.2f}s > {prev.wall:.2f}s×{wt}")
    if cur.cpu > prev.cpu * ct + 0.05:
        worse.append(f"cpu {cur.cpu:.2f}s > {prev.cpu:.2f}s×{ct}")
    if cur.rss > prev.rss * rt + 16 * MIB:
        worse.append(f"rss {cur.rss // MIB}MB > {prev.rss // MIB}MB×{rt}+16")
    for m in HARM_GRADED.get(engine, ()):
        if m in prev.harm and m in cur.harm and cur.harm[m] > prev.harm[m] * ht + hs:
            worse.append(f"{m} {cur.harm[m]} > {prev.harm[m]}×{ht}+{hs}")
    return worse


def _grade(led: Ledger, engine: str, path: str, prev: Sample | None, cur: Sample | None) -> None:
    """Record one path's verdict."""
    if prev is None or cur is None:
        led.failed(engine, "-", SCEN, path, f"perf[{engine}/{path}]: a run failed "
                   f"(prev ok={prev is not None}, this ok={cur is not None})", "run failed")
        return
    worse = perf_verdict(engine, prev, cur)
    shown = (f"wall {cur.wall:.2f}/{prev.wall:.2f}s cpu {cur.cpu:.2f}/{prev.cpu:.2f}s "
             f"rss {cur.rss // MIB}/{prev.rss // MIB}MB")
    if worse:
        led.failed(engine, "-", SCEN, path, f"perf[{engine}/{path}]: {'; '.join(worse)}", shown)
    else:
        led.passed(engine, "-", SCEN, path, f"perf[{engine}/{path}]: this/prev {shown}", shown)


def _init_dir(prev: Path, root: Path, tag: str, url: str, table: str, mode: str) -> Path | None:
    """A directory holding the previous release's `init` config for `table` in `mode`."""
    d = root / tag
    d.mkdir(parents=True)
    p = run([str(prev), "init", "--source-env", "RIVET_PERF_URL", "--table", table, "--mode", mode,
             "-o", "c.yaml"], env={"RIVET_PERF_URL": url, "RIVET_STATE_URL": ""}, cwd=d)
    return d if p.ok else None


def _fresh(d: Path) -> None:
    """Drop a directory's output and state, keeping its config."""
    shutil.rmtree(d / "output", ignore_errors=True)
    for f in d.glob(".rivet_state.db*"):
        f.unlink()


def _batch_path(binary: Path, d: Path, url: str, engine: str, table: str, path: str,
                rows: int = ROWS, idc: str = "id") -> Sample | None:
    """Warm up, then the minimum of REPS measured runs of one batch path."""
    env = {"RIVET_PERF_URL": url, "RIVET_STATE_URL": ""}
    samples: list[Sample] = []
    for i in range(REPS + 1):
        if path == "incremental":
            if i == 0:
                _fresh(d)
                run([str(binary), "run", "-c", "c.yaml"], env=env, cwd=d, timeout=None)
            base = ROWS + i * DELTA
            _sql(engine, url, _grow_sql(engine, table, base + 1, base + DELTA))
        else:
            _fresh(d)
            if path == "resume":
                run([str(binary), "run", "-c", "c.yaml"], cwd=d, timeout=None,
                    env={**env, "RIVET_TEST_PANIC_AT": "after_keyset_page:0"})
        s = _timed(binary, d, env, "run", "-c", "c.yaml")
        if i:
            samples.append(s)
    # A fast run that read nothing is not a measurement: the last output holds every row.
    got = _declared(d / "output", f"SELECT count(DISTINCT {idc}) FROM {{parts}}")
    want = rows + (REPS + 1) * DELTA if path == "incremental" else rows
    if not got or got[0][0] != want:
        return None
    return _best(samples)


def _grow_sql(engine: str, table: str, lo: int, hi: int) -> str:
    """Insert ids lo..hi with a newer cursor than any existing row."""
    if engine == "postgres":
        return (f"INSERT INTO {table} SELECT g, g, TIMESTAMPTZ '2027-01-01' + g * INTERVAL '1 second' "
                f"FROM generate_series({lo}, {hi}) g;")
    if engine == "mysql":
        return ("SET SESSION cte_max_recursion_depth = 1000000; "
                f"INSERT INTO {table} WITH RECURSIVE g(n) AS (SELECT {lo} UNION ALL SELECT n + 1 FROM g "
                f"WHERE n < {hi}) SELECT n, n, TIMESTAMP('2027-01-01') + INTERVAL n SECOND FROM g;")
    return (f"INSERT INTO {table} SELECT n, n, DATEADD(SECOND, n, CAST('2027-01-01' AS DATETIME2)) FROM "
            f"(SELECT TOP ({hi - lo + 1}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {lo - 1} AS n "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b) q;")


def _batch(led: Ledger, prev: Path, root: Path) -> None:
    for engine in ("postgres", "mysql", "mssql"):
        url = os.environ.get(f"RIVET_ORACLE_{engine.upper()}_URL", "")
        if not url:
            led.skipped(engine, "-", SCEN, "batch", f"perf[{engine}]: no RIVET_ORACLE_{engine.upper()}_URL", "no url")
            continue
        for path, mode in (("full", "full"), ("keyset", "chunked"), ("incremental", "incremental"),
                           ("resume", "chunked")):
            table = f"perf_{engine[:2]}_{path}_{os.getpid()}"
            if not _seed(engine, url, table, ROWS, with_cursor=True):
                led.failed(engine, "-", SCEN, path, f"perf[{engine}/{path}]: seed failed", "seed")
                continue
            try:
                d_prev = _init_dir(prev, root, f"{engine}_{path}_prev", url, table, mode)
                if d_prev is None:
                    led.failed(engine, "-", SCEN, path, f"perf[{engine}/{path}]: previous init failed", "init")
                    continue
                d_cur = root / f"{engine}_{path}_cur"
                d_cur.mkdir()
                shutil.copy(d_prev / "c.yaml", d_cur / "c.yaml")
                p = _batch_path(prev, d_prev, url, engine, table, path)
                if path == "incremental":
                    _seed(engine, url, table, ROWS, with_cursor=True)
                c = _batch_path(rivet_bin(), d_cur, url, engine, table, path)
                _grade(led, engine, path, p, c)
            finally:
                _sql(engine, url, f"DROP TABLE IF EXISTS {table};")


def _pair(led: Ledger, prev: Path, root: Path, engine: str, url: str, table: str, mode: str,
          label: str, rows: int = ROWS, idc: str = "id", init_url: str | None = None) -> None:
    """Grade one path: the previous release's init config, run by both binaries."""
    d_prev = _init_dir(prev, root, f"{engine}_{label}_prev".replace("@", "_"), init_url or url, table, mode)
    if d_prev is None:
        led.failed(engine, "-", SCEN, label, f"perf[{engine}/{label}]: previous init failed", "init")
        return
    d_cur = root / f"{engine}_{label}_cur".replace("@", "_")
    d_cur.mkdir()
    shutil.copy(d_prev / "c.yaml", d_cur / "c.yaml")
    path = "keyset" if mode == "chunked" else "full"
    _grade(led, engine, label, _batch_path(prev, d_prev, url, engine, table, path, rows, idc),
           _batch_path(rivet_bin(), d_cur, url, engine, table, path, rows, idc))


def _off_happy_path(led: Ledger, prev: Path, root: Path) -> None:
    """A 50 ms link, a table ten times larger, and a document store — the paths a lab never sees."""
    from .failure import PG_TOXI_URL, TOXI_PROXY, _toxi, _toxi_lock

    url = os.environ.get("RIVET_ORACLE_POSTGRES_URL", "")
    if url:
        table = f"perf_pg_lat_{os.getpid()}"
        if _seed("postgres", url, table, ROWS, with_cursor=False):
            try:
                with _toxi_lock():
                    _toxi("POST", "/proxies", {"name": TOXI_PROXY, "listen": "0.0.0.0:15432",
                                               "upstream": "postgres:5432", "enabled": True})
                    code = _toxi("POST", f"/proxies/{TOXI_PROXY}/toxics",
                                 {"name": "perf_latency", "type": "latency", "stream": "downstream",
                                  "attributes": {"latency": 50}})
                    try:
                        if code != 200:
                            led.failed("postgres", "-", SCEN, "latency", f"perf[postgres/@50ms]: "
                                       f"toxiproxy refused the latency toxic (HTTP {code})", "toxi")
                        else:
                            for mode, label in (("full", "full@50ms"), ("chunked", "keyset@50ms")):
                                _pair(led, prev, root, "postgres", PG_TOXI_URL, table, mode, label)
                    finally:
                        _toxi("DELETE", f"/proxies/{TOXI_PROXY}/toxics/perf_latency")
            finally:
                _sql("postgres", url, f"DROP TABLE IF EXISTS {table};")
        big = f"perf_pg_big_{os.getpid()}"
        if _seed("postgres", url, big, BIG_ROWS, with_cursor=False):
            try:
                _pair(led, prev, root, "postgres", url, big, "chunked", "keyset-2M", rows=BIG_ROWS)
            finally:
                _sql("postgres", url, f"DROP TABLE IF EXISTS {big};")
    murl = os.environ.get("RIVET_ORACLE_MONGO_URL", "")
    if murl:
        from .cdc import _mongosh

        coll = f"perf_mg_{os.getpid()}"
        _mongosh(murl, f"db.{coll}.drop(); db.{coll}.insertMany(Array.from({{length: {ROWS}}}, "
                       f"(_, i) => ({{_id: i + 1, v: i, meta: {{k: i}}}})));")
        try:
            _pair(led, prev, root, "mongo", murl, coll, "full", "full", idc="CAST(_id AS BIGINT)")
        finally:
            _mongosh(murl, f"db.{coll}.drop();")


def _cdc_changes(engine: str, url: str, lo: int) -> None:
    """CDC_CHANGES changes from id `lo`: one big transaction of inserts, then updates and deletes."""
    from .cdc import _mongosh, _mssql_max_lsn, _mysql, _psql, _sqlcmd, _wait_mssql_captured

    hi = lo + CDC_CHANGES - 1
    if engine == "postgres":
        _psql(url, sql=(f"INSERT INTO orc_cdc_probe SELECT g, g, jsonb_build_object('k', g) "
                        f"FROM generate_series({lo}, {hi}) g;\n"
                        f"UPDATE orc_cdc_probe SET amount = amount + 1 WHERE id BETWEEN {lo} AND {hi} AND id % 10 = 0;\n"
                        f"DELETE FROM orc_cdc_probe WHERE id BETWEEN {lo} AND {hi} AND id % 17 = 0;\n"))
    elif engine == "mysql":
        _mysql(url, ("SET SESSION cte_max_recursion_depth = 1000000; "
                     f"INSERT INTO orc_cdc_probe WITH RECURSIVE g(n) AS (SELECT {lo} UNION ALL SELECT n + 1 FROM g "
                     f"WHERE n < {hi}) SELECT n, n, JSON_OBJECT('k', n) FROM g; "
                     f"UPDATE orc_cdc_probe SET amount = amount + 1 WHERE id BETWEEN {lo} AND {hi} AND id % 10 = 0; "
                     f"DELETE FROM orc_cdc_probe WHERE id BETWEEN {lo} AND {hi} AND id % 17 = 0;"))
    elif engine == "mssql":
        before = _mssql_max_lsn(url)
        _sqlcmd(url, sql=(f"INSERT INTO dbo.orc_cdc_probe SELECT n, n, CONCAT('{{\"k\":', n, '}}') FROM "
                          f"(SELECT TOP ({CDC_CHANGES}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {lo - 1} AS n "
                          "FROM sys.all_objects a CROSS JOIN sys.all_objects b) q;\n"
                          f"UPDATE dbo.orc_cdc_probe SET amount = amount + 1 WHERE id BETWEEN {lo} AND {hi} AND id % 10 = 0;\n"
                          f"DELETE FROM dbo.orc_cdc_probe WHERE id BETWEEN {lo} AND {hi} AND id % 17 = 0;\n"))
        _wait_mssql_captured(url, before)
    else:
        _mongosh(url, (f"db.orc_cdc_probe.insertMany(Array.from({{length: {CDC_CHANGES}}}, "
                       f"(_, i) => ({{_id: {lo} + i, amount: i, meta: {{k: i}}}}))); "
                       f"db.orc_cdc_probe.updateMany({{_id: {{$gte: {lo}, $lte: {hi}}}, amount: {{$mod: [10, 0]}}}}, "
                       "{$inc: {amount: 1}}); "
                       f"db.orc_cdc_probe.deleteMany({{_id: {{$gte: {lo}, $lte: {hi}}}, amount: {{$mod: [17, 0]}}}});"))


def _cdc_side(binary: Path, engine: str, url: str, path: str = "cdc") -> Sample | None:
    """Anchor, then the minimum of REPS measured drains of CDC_CHANGES changes each (warm-up first).

    `cdc-spill` caps the transaction buffer so the big transaction spills to disk;
    `cdc-resume` crashes each drain after its flush and times the run that resumes it.
    """
    from .cdc import _ENGINES, _workdir

    eng = _ENGINES[engine]
    work = _workdir()
    block = eng.setup(url, work)
    if block is None:
        return None
    tls = "\n  tls: { accept_invalid_certs: true }" if engine == "mssql" else ""
    (work / "c.yaml").write_text(
        f"source:\n  type: {engine}\n  url: \"{url}\"{tls}\nexports:\n  - name: orc_cdc_probe\n"
        f"    table: orc_cdc_probe\n    mode: cdc\n    format: parquet\n    {block}\n"
        "    destination:\n      type: local\n      path: ./output/\n"
    )
    env = {"RIVET_STATE_URL": ""}
    if path == "cdc-spill":
        env |= {"RIVET_CDC_MAX_TX_ROWS": str(CDC_CHANGES // 20), "RIVET_CDC_SPILL_DIR": "1"}
    try:
        run([str(binary), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        samples = []
        for i in range(REPS + 1):
            _cdc_changes(engine, url, 1 + i * CDC_CHANGES)
            if path == "cdc-resume":
                run([str(binary), "run", "-c", "c.yaml"], cwd=work, timeout=None,
                    env={**env, "RIVET_TEST_PANIC_AT": "cdc_after_flush_before_ack"})
            s = _timed(binary, work, env, "run", "-c", "c.yaml")
            if i:
                samples.append(s)
        # A fast drain that captured nothing is not a measurement: every inserted id must have
        # landed. DISTINCT ids, not events — updates and deletes would cover lost inserts.
        got = _declared(work / "output", f"SELECT count(DISTINCT {eng.id_col}) FROM {{parts}}")
        if not got or got[0][0] < (REPS + 1) * CDC_CHANGES:
            return None
        return _best(samples)
    finally:
        eng.cleanup(url, work)


def _cdc(led: Ledger, prev: Path) -> None:
    for engine in ("postgres", "mysql", "mssql", "mongo"):
        url = os.environ.get(f"RIVET_CDC_{engine.upper()}_URL", "")
        if not url:
            led.skipped(engine, "-", SCEN, "cdc", f"perf[{engine}/cdc]: no RIVET_CDC_{engine.upper()}_URL", "no url")
            continue
        # MongoDB has no transaction buffer, so there is nothing for it to spill.
        for path in ("cdc", "cdc-resume") if engine == "mongo" else ("cdc", "cdc-spill", "cdc-resume"):
            _grade(led, engine, path, _cdc_side(prev, engine, url, path),
                   _cdc_side(rivet_bin(), engine, url, path))


def _aa(led: Ledger, prev: Path, root: Path) -> None:
    """A/A: the previous release against ITSELF must pass these tolerances, or this machine is
    too noisy for them and a red here would blame the product for the bench."""
    url = os.environ.get("RIVET_ORACLE_POSTGRES_URL", "")
    cdc_url = os.environ.get("RIVET_CDC_POSTGRES_URL", "")
    if not url or not cdc_url:
        led.skipped("postgres", "-", SCEN, "a/a", "perf A/A: no postgres URLs", "no url")
        return
    table = f"perf_aa_{os.getpid()}"
    if not _seed("postgres", url, table, ROWS, with_cursor=True):
        led.failed("postgres", "-", SCEN, "a/a", "perf A/A: seed failed", "seed")
        return
    try:
        d = _init_dir(prev, root, "aa_full", url, table, "full")
        pairs = [("full", _batch_path(prev, d, url, "postgres", table, "full") if d else None,
                  _batch_path(prev, d, url, "postgres", table, "full") if d else None),
                 ("cdc", _cdc_side(prev, "postgres", cdc_url), _cdc_side(prev, "postgres", cdc_url))]
    finally:
        _sql("postgres", url, f"DROP TABLE IF EXISTS {table};")
    for path, a, b in pairs:
        if a is None or b is None:
            led.failed("postgres", "-", SCEN, "a/a", f"perf A/A[{path}]: a run failed", "run failed")
            continue
        noise = perf_verdict("postgres", a, b) + perf_verdict("postgres", b, a)
        if noise:
            led.failed("postgres", "-", SCEN, "a/a", f"perf A/A[{path}]: the previous release differs "
                       f"from ITSELF past the tolerances ({'; '.join(noise)}) — this machine is too "
                       "noisy for them; a perf verdict here would measure the bench", "noisy")
        else:
            led.passed("postgres", "-", SCEN, "a/a", f"perf A/A[{path}]: the previous release matches "
                       f"itself within the tolerances (wall {a.wall:.2f}/{b.wall:.2f}s)", "a/a")


def verify_perf_regression(led: Ledger) -> None:
    """Wall, CPU, peak RSS and source harm per path, this binary against the previous release."""
    prev = _require_prev_binary(led, "all", "-", SCEN, "local", "perf regression")
    if prev is None:
        return
    led.phase("Perf regression vs prev — batch paths and CDC drains: wall, CPU, RSS, source harm")
    root = Path(tempfile.mkdtemp(prefix="rivet-oracle-perf-"))
    _aa(led, prev, root)
    _batch(led, prev, root)
    _off_happy_path(led, prev, root)
    _cdc(led, prev)
