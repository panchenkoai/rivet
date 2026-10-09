#!/usr/bin/env python3
"""Oracle baselines for docs/perf-matrix.yaml: one run measures every Oracle cell.

CDC (LogMiner, as the stand's common capture user):
  capture throughput   rows/s of one bounded drain over a seeded backlog
  interval RSS         peak RSS of a drain as its backlog grows 36x
  co-tenancy           a committing writer's rows/s without and with a looping drain, alternated
  retention            what the reader registers with the server (nothing pins redo)
Batch (as the stand's app user):
  memory               peak RSS of an export at two table sizes 12x apart
  hot-source harm      point-read p99 and the longest statement seen running, during an export

Every config is written by `rivet init`; the measurements read the manifests rivet wrote and
the server's own views (V$SESSION, DBA_CAPTURE), never rivet's summary line.

Usage: RIVET_BIN=target/release/rivet uv run python dev/cdc/harm_oracle.py [secs-per-phase]
"""

from __future__ import annotations

import glob
import json
import os
import random
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import oracledb

DSN = os.environ.get("ORACLE_DSN", "127.0.0.1:1521/FREEPDB1")
APP_URL = f"oracle://rivet:rivet@{DSN}"
CDC_URL = f"oracle://c%23%23rivetcdc:rivet@{DSN}"
RIVET_BIN = os.path.abspath(os.environ.get("RIVET_BIN", "target/release/rivet"))
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 15
ROUNDS = int(os.environ.get("HARM_ORA_ROUNDS", "3"))
WRITER_BATCH = int(os.environ.get("HARM_ORA_WRITER_BATCH", "200"))
TAG = f"{os.getpid()}"
CDC_T, BATCH_T, OLTP_T = f"HARM_CDC_{TAG}", f"HARM_BATCH_{TAG}", f"HARM_OLTP_{TAG}"
BACKLOGS = tuple(int(n) for n in os.environ.get("HARM_ORA_BACKLOGS", "20000,60000,240000,720000").split(","))
BATCH_ROWS = tuple(int(n) for n in os.environ.get("HARM_ORA_BATCH_ROWS", "150000,1800000").split(","))
APP = {"user": "rivet", "password": "rivet", "dsn": DSN}
SYSTEM = {"user": "system", "password": "rivet", "dsn": DSN}


def sql(con: oracledb.Connection, text: str, *binds: object) -> list[tuple]:
    with con.cursor() as cur:
        cur.execute(text, binds)
        rows = cur.fetchall() if cur.description else []
    con.commit()
    return rows


def timed_run(cwd: str, cfg: str, env: dict) -> tuple[float, float]:
    """One `rivet run`: (wall seconds, peak RSS in MB), from /usr/bin/time -l."""
    t0 = time.monotonic()
    p = subprocess.run(["/usr/bin/time", "-l", RIVET_BIN, "run", "--config", cfg],
                       cwd=cwd, env={**os.environ, **env}, capture_output=True, text=True)
    wall = time.monotonic() - t0
    if p.returncode != 0:
        raise SystemExit(f"rivet run failed in {cwd}:\n{p.stderr[-2000:]}")
    m = re.search(r"(\d+)\s+maximum resident set size", p.stderr)
    if not m:
        raise SystemExit("no `maximum resident set size` line from /usr/bin/time -l")
    return wall, int(m.group(1)) / 1_048_576


def init(table: str, url: str, extra: list[str]) -> tuple[str, dict]:
    """`rivet init` in a fresh directory: (the directory, the env its config reads the URL from)."""
    d = tempfile.mkdtemp(prefix="harm_ora_")
    env = {"HARM_ORA_URL": url}
    p = subprocess.run([RIVET_BIN, "init", "--source-env", "HARM_ORA_URL", "--table", table, *extra,
                        "-o", "rivet.yaml"], cwd=d, env={**os.environ, **env}, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"rivet init {table} failed:\n{p.stderr[-2000:]}")
    return d, env


def manifest_rows(cwd: str) -> int:
    """Rows the run-unique manifest copies under the config's output declare."""
    total = 0
    for path in glob.glob(os.path.join(cwd, "**", "manifest-*.json"), recursive=True):
        with open(path) as fh:
            doc = json.load(fh)
        if str(doc.get("status", "success")).lower() == "success":
            total += int(doc.get("row_count") or 0)
    return total


def seed(con: oracledb.Connection, table: str, lo: int, hi: int, per_commit: int = 1000) -> None:
    """Rows lo..=hi, committed every `per_commit` (many small transactions, as a writer makes them)."""
    with con.cursor() as cur:
        for start in range(lo, hi + 1, per_commit):
            cur.executemany(f"INSERT INTO {table} (id, v, pad) VALUES (:1, :2, RPAD('x', 100, 'x'))",
                            [(i, i) for i in range(start, min(start + per_commit, hi + 1))])
            con.commit()


class Writer(threading.Thread):
    """`WRITER_BATCH` rows per commit until stopped; `rows` is what it committed."""

    def __init__(self, table: str, base: int):
        super().__init__(daemon=True)
        self.table, self.next, self.rows, self.stop = table, base, 0, threading.Event()

    def run(self) -> None:
        with oracledb.connect(**APP) as con, con.cursor() as cur:
            while not self.stop.is_set():
                cur.executemany(f"INSERT INTO {self.table} (id, v, pad) VALUES (:1, :2, RPAD('x', 100, 'x'))",
                                [(i, i) for i in range(self.next, self.next + WRITER_BATCH)])
                con.commit()
                self.next += WRITER_BATCH
                self.rows += WRITER_BATCH


def cdc_cells() -> dict:
    with oracledb.connect(**APP) as app, oracledb.connect(**SYSTEM) as system:
        sql(app, f"CREATE TABLE {CDC_T} (id NUMBER(18) PRIMARY KEY, v NUMBER(18), pad VARCHAR2(100))")
        sql(app, f"ALTER TABLE {CDC_T} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
        sql(app, f"GRANT SELECT ON {CDC_T} TO c##rivetcdc")
        out: dict = {}
        try:
            cwd, env = init(f"RIVET.{CDC_T}", CDC_URL, ["--mode", "cdc"])
            timed_run(cwd, "rivet.yaml", env)  # anchor
            next_id, delivered, drains = 1, 0, []
            for backlog in BACKLOGS:
                seed(app, CDC_T, next_id, next_id + backlog - 1)
                next_id += backlog
                wall, rss = timed_run(cwd, "rivet.yaml", env)
                got = manifest_rows(cwd) - delivered
                delivered += got
                if got != backlog:
                    raise SystemExit(f"drain over {backlog} rows declared {got}")
                drains.append({"backlog": backlog, "wall_s": round(wall, 2), "rows_per_s": round(backlog / wall),
                               "peak_rss_mb": round(rss, 1)})
                print(f"cdc drain: {drains[-1]}", flush=True)
            out["drains"] = drains

            bases = iter(range(10_000_000, 10**9, 10_000_000))

            def writer_rate(with_drain: bool) -> float:
                stop = threading.Event()

                def loop() -> None:
                    while not stop.is_set():
                        subprocess.run([RIVET_BIN, "run", "--config", "rivet.yaml"], cwd=cwd,
                                       env={**os.environ, **env}, capture_output=True, text=True)

                drain = threading.Thread(target=loop, daemon=True)
                if with_drain:
                    drain.start()
                w = Writer(CDC_T, next(bases))
                w.start()
                time.sleep(SECS)
                w.stop.set()
                w.join()
                stop.set()
                if with_drain:
                    drain.join(timeout=300)
                return w.rows / SECS

            # Alternated, so drift in the server's own commit rate lands on both sides.
            pairs = [(writer_rate(False), writer_rate(True)) for _ in range(ROUNDS)]
            base, under = (statistics.median(p[i] for p in pairs) for i in (0, 1))
            out["writer_rows_per_s"] = {"baseline": round(base), "under_drain": round(under),
                                        "throughput_x": round(under / base, 2),
                                        "rounds": [[round(b), round(u)] for b, u in pairs]}
            print(f"cdc co-tenancy: {out['writer_rows_per_s']}", flush=True)
            timed_run(cwd, "rivet.yaml", env)  # drain what the writers left
            out["retention"] = {
                "dba_capture_rows": sql(system, "SELECT COUNT(*) FROM dba_capture")[0][0],
                "logminer_sessions_left": sql(system, "SELECT COUNT(*) FROM v$logmnr_session")[0][0],
                "capture_user_sessions_left": sql(
                    system, "SELECT COUNT(*) FROM v$session WHERE username = 'C##RIVETCDC'")[0][0],
            }
            print(f"cdc retention: {out['retention']}", flush=True)
        finally:
            sql(app, f"DROP TABLE {CDC_T} PURGE")
        return out


def batch_cells() -> dict:
    with oracledb.connect(**APP) as app, oracledb.connect(**SYSTEM) as system:
        sql(app, f"CREATE TABLE {BATCH_T} (id NUMBER(18) PRIMARY KEY, v NUMBER(18), pad VARCHAR2(100))")
        sql(app, f"CREATE TABLE {OLTP_T} (id NUMBER(18) PRIMARY KEY, v NUMBER(18))")
        sql(app, f"INSERT INTO {OLTP_T} SELECT LEVEL, LEVEL FROM dual CONNECT BY LEVEL <= 10000")
        out: dict = {"exports": []}
        try:
            have = 0
            for rows in BATCH_ROWS:
                for lo in range(have + 1, rows + 1, 100_000):
                    sql(app, f"INSERT INTO {BATCH_T} SELECT {lo} - 1 + LEVEL, LEVEL, RPAD('x', 100, 'x') "
                             f"FROM dual CONNECT BY LEVEL <= {min(100_000, rows - lo + 1)}")
                have = rows
                sql(app, f"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '{BATCH_T}'); END;")
                cwd, env = init(BATCH_T, APP_URL, [])
                mode = re.search(r"^\s*mode:\s*(\S+)", open(os.path.join(cwd, "rivet.yaml")).read(), re.M).group(1)

                lat: list[float] = []
                probing, longest = threading.Event(), [0.0]
                mine = {r[0] for r in sql(system, "SELECT sid FROM v$session WHERE audsid = SYS_CONTEXT('USERENV','SESSIONID')")}

                def probe(sink: list[float]) -> None:
                    with oracledb.connect(**APP) as con, con.cursor() as cur:
                        while probing.is_set():
                            t0 = time.perf_counter()
                            cur.execute(f"SELECT v FROM {OLTP_T} WHERE id = :1", [random.randint(1, 10000)])
                            cur.fetchall()
                            sink.append(time.perf_counter() - t0)

                def watch() -> None:
                    with oracledb.connect(**SYSTEM) as con:
                        while probing.is_set():
                            r = sql(con, "SELECT NVL(MAX((SYSDATE - sql_exec_start) * 86400), 0) FROM v$session "
                                         "WHERE username = 'RIVET' AND status = 'ACTIVE' AND sql_exec_start IS NOT NULL "
                                         f"AND sql_id IN (SELECT sql_id FROM v$sql WHERE sql_text LIKE '%{BATCH_T}%' "
                                         "AND sql_text NOT LIKE '%v$sql%')")
                            longest[0] = max(longest[0], float(r[0][0]))
                            time.sleep(0.1)

                base: list[float] = []
                probing.set()
                t = threading.Thread(target=probe, args=(base,), daemon=True)
                t.start()
                time.sleep(min(SECS, 5))
                probing.clear()
                t.join()

                probing.set()
                threads = [threading.Thread(target=probe, args=(lat,), daemon=True), threading.Thread(target=watch, daemon=True)]
                for th in threads:
                    th.start()
                wall, rss = timed_run(cwd, "rivet.yaml", env)
                probing.clear()
                for th in threads:
                    th.join()
                got = manifest_rows(cwd)
                if got != rows:
                    raise SystemExit(f"export of {rows} rows declared {got}")
                p99 = lambda xs: statistics.quantiles(xs, n=100)[98] * 1000  # noqa: E731
                cell = {"rows": rows, "mode": mode, "wall_s": round(wall, 2), "rows_per_s": round(rows / wall),
                        "peak_rss_mb": round(rss, 1), "oltp_p99_ms": {"baseline": round(p99(base), 2), "under_export": round(p99(lat), 2)},
                        "oltp_p99_x": round(p99(lat) / p99(base), 2), "longq_s": round(longest[0], 1), "mine": len(mine)}
                out["exports"].append(cell)
                print(f"batch export: {cell}", flush=True)
        finally:
            sql(app, f"DROP TABLE {BATCH_T} PURGE")
            sql(app, f"DROP TABLE {OLTP_T} PURGE")
        return out


def main() -> None:
    if not os.path.exists(RIVET_BIN):
        raise SystemExit(f"no rivet binary at {RIVET_BIN} (set RIVET_BIN)")
    print(f"Oracle perf baselines, {SECS}s per timed phase, binary {RIVET_BIN}", flush=True)
    report = {"cdc": cdc_cells(), "batch": batch_cells()}
    print("HARM-ORACLE-REPORT " + json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
