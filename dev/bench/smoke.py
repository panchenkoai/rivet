#!/usr/bin/env python3
"""Benchmark smoke runner — reads docs/bench/matrix.yaml, runs each tool over the
seeded smoke fixture, and prints THREE matrices: benchmark, harm, type-loss.

The metric SET lives once in matrix.yaml (benchmark_metrics + harm_metrics); this
file provides the capture per key and a guard that fails if the two drift. Run
with the system python — it has PyYAML + dlt (the homebrew pythons don't):

    /usr/bin/python3 dev/bench/smoke.py                 # content_items, all tools
    /usr/bin/python3 dev/bench/smoke.py --table orders  # a different smoke table

Each tool runs once, serially, capturing in that one pass: perf (wall via
`/usr/bin/time -l`, rows, rows/s, peak RSS, out MB), source-harm (cumulative
counters + gauge peaks + a co-running OLTP p99 probe), and type fidelity
(parquet column family vs the source). Smokes first: a tiny fixture, so a full
round-trip is a couple of minutes; harm signals that need scale (temp spill,
xmin hold) stay ~0 until the full run.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "dev" / "bench" / ".smoke-out"

# Bench fixtures live in a dedicated `rivet_bench` DB per engine so the live-test
# `rivet` fixtures are never touched. Host → docker-mapped ports.
PG = {
    "host": "localhost", "port": "5432", "db": "rivet_bench",
    "user": "rivet", "pw": "rivet",
}
PG_URL = f"postgresql://{PG['user']}:{PG['pw']}@{PG['host']}:{PG['port']}/{PG['db']}"
# Docker postgres has no TLS; sling/ingestr default to sslmode=require and must
# be told to disable it (rivet/duckdb/odbc2parquet don't force TLS on loopback).
PG_URL_NOSSL = PG_URL + "?sslmode=disable"

# The engine under test is chosen with --engine and stamped into ENG at startup.
# Each engine config carries its container, admin SQL runner, harm SQL (counters
# / gauges / cache / dead), source-types query, OLTP-probe client, and the
# per-tool connection URL. The harness logic is engine-agnostic; only these
# strings differ. Postgres is the reference; mysql/mssql/mongo slot in beside it.
ENG = {}  # set in main() from ENGINES[args.engine]


def _docker_sql(container, argv, q):
    return sh(["docker", "exec", "-i", container] + argv, input=q).stdout.strip()


# ── PostgreSQL engine ────────────────────────────────────────────────────────
_PG_ADMIN = ["psql", "-U", "rivet", "-d", "rivet_bench", "-tAqc", ""]  # q via stdin


def _pg_sql(q):
    return sh(["docker", "exec", "rivet-mongo-postgres-1", "psql", "-U", "rivet",
               "-d", "rivet_bench", "-tAc", q]).stdout.strip()


def _my_sql(q):  # root — perf_schema / data_locks need it; tools connect as rivet
    return sh(["docker", "exec", "rivet-mongo-mysql-1", "mysql", "-uroot", "-privet",
               "rivet_bench", "-N", "-e", q], input="").stdout.strip()


PG_ENGINE = {
    "name": "postgres",
    "sql": _pg_sql,
    "counter_keys": ["tup_returned", "tup_fetched", "temp_bytes", "blks_read",
                     "blk_read_time", "wal_bytes"],
    "counters_sql": (
        "SELECT tup_returned||','||tup_fetched||','||temp_bytes||','||blks_read"
        "||','||blk_read_time||','||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0') "
        "FROM pg_stat_database WHERE datname='rivet_bench'"),
    "gauge_sql": (
        "SELECT COALESCE(EXTRACT(EPOCH FROM max(now()-query_start) "
        "FILTER (WHERE state='active')),0)||','||"
        "COALESCE(EXTRACT(EPOCH FROM max(now()-xact_start)),0)||','||count(*)||','||"
        "(SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a2 ON l.pid=a2.pid "
        "WHERE a2.datname='rivet_bench' AND a2.pid<>pg_backend_pid() "
        "AND a2.application_name<>'harness_probe')||','||"
        "(SELECT count(*) FROM pg_stat_activity a3 WHERE a3.datname='rivet_bench' "
        "AND cardinality(pg_blocking_pids(a3.pid))>0)||','||"
        "COALESCE(max(age(backend_xmin)),0) "
        "FROM pg_stat_activity WHERE datname='rivet_bench' AND pid<>pg_backend_pid() "
        "AND backend_type='client backend' AND application_name<>'harness_probe'"),
    "cache_sql": ("SELECT count(*)*8/1024.0 FROM pg_buffercache b JOIN pg_class c "
                  "ON b.relfilenode = pg_relation_filenode(c.oid) WHERE c.relname='{t}'"),
    "dead_sql": ("SELECT COALESCE(n_dead_tup,0) FROM pg_stat_user_tables "
                 "WHERE relname='{t}'"),
    "types_sql": ("SELECT column_name||'='||data_type FROM information_schema.columns "
                  "WHERE table_name='{t}' ORDER BY ordinal_position"),
    "setup": lambda: [_pg_sql("CREATE EXTENSION IF NOT EXISTS pg_buffercache"),
                      _pg_sql("CREATE TABLE IF NOT EXISTS bench_probe(id int PRIMARY KEY, v int)"),
                      _pg_sql("INSERT INTO bench_probe SELECT g,g FROM generate_series(1,1000) g "
                              "ON CONFLICT DO NOTHING")],
    # OLTP probe: persistent psql, server-side \timing (DB latency, not docker).
    "probe_argv": ["docker", "exec", "-i", "rivet-mongo-postgres-1", "psql", "-U",
                   "rivet", "-d", "rivet_bench", "-qAt"],
    "probe_mode": "persistent",
    "probe_init": "SET application_name='harness_probe';\n\\timing on\n",
    "probe_query": "SELECT v FROM bench_probe WHERE id={i};\n",
    "probe_time_re": r"Time:\s+([\d.]+)\s+ms",
    "container": "rivet-mongo-postgres-1",
    "url": lambda tool: PG_URL_NOSSL if tool in ("sling", "ingestr") else PG_URL,
    # Per-tool connection recipes (how each tool READS this engine).
    "rivet_type": "postgres",
    "duckdb_install": "INSTALL postgres; LOAD postgres;",
    "duckdb_attach": (f"ATTACH 'host={PG['host']} port={PG['port']} dbname={PG['db']} "
                      f"user={PG['user']} password={PG['pw']}' AS src (TYPE postgres, READ_ONLY)"),
    "clickhouse_fn": (lambda t: f"postgresql('{PG['host']}:{PG['port']}', '{PG['db']}', "
                                f"'{t}', '{PG['user']}', '{PG['pw']}')"),
    "odbc_conn": (f"Driver={{PostgreSQL Unicode}};Server={PG['host']};Port={PG['port']};"
                  f"Database={PG['db']};Uid={PG['user']};Pwd={PG['pw']};"),
    "sling_url": PG_URL_NOSSL,
    "ingestr_url": PG_URL_NOSSL,
    "qualify": lambda t: f"public.{t}",       # sling/ingestr stream name
    "dlt_url": PG_URL, "dlt_schema": "public",
}

# ── MySQL engine ─────────────────────────────────────────────────────────────
MY_ENGINE = {
    "name": "mysql",
    "sql": _my_sql,
    # SHOW GLOBAL STATUS deltas (the matrix.yaml harm_metrics.mysql set).
    "counter_keys": ["Innodb_rows_read", "Innodb_buffer_pool_reads",
                     "Innodb_buffer_pool_read_requests", "Innodb_log_waits",
                     "tmp_disk_tables", "Handler_read_rnd_next"],
    "counters_sql": (
        "SELECT CONCAT_WS(',',"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_rows_read'),"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_reads'),"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_read_requests'),"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_log_waits'),"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Created_tmp_disk_tables'),"
        "(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Handler_read_rnd_next'))"),
    # gauge: longest active query (s), longest txn (s), conns, locks, blocked, xmin(0)
    "gauge_sql": (
        "SELECT CONCAT_WS(',',"
        "IFNULL((SELECT MAX(esc.TIMER_WAIT)/1e12 FROM performance_schema.events_statements_current esc "
        "JOIN performance_schema.threads t ON t.thread_id=esc.thread_id "
        "WHERE t.processlist_db='rivet_bench' AND t.processlist_user='rivet'),0),"
        "IFNULL((SELECT MAX(TIMESTAMPDIFF(SECOND,trx_started,NOW())) FROM information_schema.innodb_trx),0),"
        "(SELECT COUNT(*) FROM information_schema.processlist WHERE db='rivet_bench' AND user='rivet'),"
        "(SELECT COUNT(*) FROM performance_schema.data_locks WHERE object_schema='rivet_bench'),"
        "(SELECT COUNT(*) FROM performance_schema.data_lock_waits),"
        "0)"),
    "cache_sql": None,   # innodb buffer-page introspection is heavy; skip (→0)
    "dead_sql": None,    # no dead-tuple concept exposed like pg
    "types_sql": ("SELECT CONCAT(column_name,'=',data_type) FROM information_schema.columns "
                  "WHERE table_schema='rivet_bench' AND table_name='{t}' ORDER BY ordinal_position"),
    "setup": lambda: [_my_sql("CREATE TABLE IF NOT EXISTS bench_probe(id int PRIMARY KEY, v int)"),
                      _my_sql("INSERT IGNORE INTO bench_probe (id,v) SELECT n,n FROM "
                              "(WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<1000) "
                              "SELECT n FROM s) x")],
    # OLTP probe: per-query docker exec, client-timed (mysql batch-buffers a
    # persistent stdin, so the psql line-protocol can't be reused). The idle
    # baseline carries the same docker overhead, so the ratio isolates the DB.
    "probe_mode": "perquery",
    "probe_cmd": ["docker", "exec", "rivet-mongo-mysql-1", "mysql", "-urivet",
                  "-privet", "rivet_bench", "-NB", "-e",
                  "SELECT v FROM bench_probe WHERE id={i}"],
    "container": "rivet-mongo-mysql-1",
    "url": lambda tool: "mysql://rivet:rivet@localhost:3306/rivet_bench",
    "rivet_type": "mysql",
    "duckdb_install": "INSTALL mysql; LOAD mysql;",
    "duckdb_attach": ("ATTACH 'host=localhost port=3306 user=rivet password=rivet "
                      "database=rivet_bench' AS src (TYPE mysql, READ_ONLY)"),
    # 127.0.0.1 (not localhost) forces TCP — 'localhost' makes clickhouse try the
    # unix socket /tmp/mysql.sock, which the docker mysql doesn't expose to the host.
    "clickhouse_fn": (lambda t: f"mysql('127.0.0.1:3306', 'rivet_bench', '{t}', "
                                "'rivet', 'rivet')"),
    # MariaDB Unicode ODBC driver (MySQL-compatible): brew install
    # mariadb-connector-odbc, then register the .dylib with `odbcinst -i -d`.
    "odbc_conn": ("Driver={MariaDB Unicode};Server=127.0.0.1;Port=3306;"
                  "Database=rivet_bench;User=rivet;Password=rivet;"),
    "sling_url": "mysql://rivet:rivet@localhost:3306/rivet_bench",
    "ingestr_url": "mysql://rivet:rivet@localhost:3306/rivet_bench",
    "qualify": lambda t: f"rivet_bench.{t}",
    "dlt_url": "mysql+pymysql://rivet:rivet@localhost:3306/rivet_bench",
    "dlt_schema": "rivet_bench",
}

ENGINES = {"postgres": PG_ENGINE, "mysql": MY_ENGINE}


def sh(cmd, **kw):
    return subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True, **kw)


def parquet_rows(path: Path) -> int:
    """Row count across path (a file or a dir of *.parquet), via the duckdb CLI."""
    glob = str(path / "**/*.parquet") if path.is_dir() else str(path)
    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"SELECT count(*) FROM read_parquet('{glob}')"])
    m = re.search(r"\d+", r.stdout or "")
    return int(m.group()) if m else -1


def out_bytes(path: Path) -> int:
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*.parquet"))
    return path.stat().st_size if path.exists() else 0


# ─── Source-harm capture ─────────────────────────────────────────────────────
# Every axis a principal DBA needs to judge how much an extraction HURTS the
# source, captured in the SAME serial run as perf. Three kinds:
#   • cumulative counters (before→after delta): read amplification, temp spill,
#     disk reads/time, WAL generated, source CPU.
#   • point-in-time gauges (peak + p50 over a 50 ms poller): longest query / txn,
#     held locks, blocked sessions, connections, xmin horizon held back (vacuum
#     starvation), buffer-cache footprint.
#   • ground truth: a co-running OLTP probe — p99 latency degradation of a real
#     point-read workload while the extraction runs (the number a DBA signs off).
def _psql(q: str) -> str:          # name kept; delegates to the selected engine
    return ENG["sql"](q)


def setup_harm():
    """Idempotent prerequisites (OLTP probe table + any cache view), per engine."""
    ENG["setup"]()


def counters() -> dict:
    """Cumulative engine counters for before/after deltas — keys per engine."""
    try:
        vals = [float(v) for v in _psql(ENG["counters_sql"]).split(",")]
        return dict(zip(ENG["counter_keys"], vals))
    except (ValueError, AttributeError):
        return {}


def cgroup_cpu_usec() -> int:
    """Source-container CPU microseconds consumed (cgroup v2 cpu.stat)."""
    r = sh(["docker", "exec", ENG["container"], "sh", "-c",
            "awk '/usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat"])
    try:
        return int(r.stdout.strip() or 0)
    except ValueError:
        return 0


def cache_footprint_mb(table: str) -> float:
    """MB of the buffer pool holding the table after the run (cache pollution).
    None for engines whose buffer-page introspection is too heavy → 0."""
    if not ENG.get("cache_sql"):
        return 0.0
    try:
        return float(_psql(ENG["cache_sql"].format(t=table)) or 0)
    except (ValueError, TypeError):
        return 0.0


def dead_tuples(table: str) -> int:
    if not ENG.get("dead_sql"):
        return 0
    try:
        return int(_psql(ENG["dead_sql"].format(t=table)) or 0)
    except ValueError:
        return 0


# ── Point-in-time gauges (peak + p50 over a background poller) ───────────────
# Each sample: longest active query (s), longest open txn (s), connections, held
# locks, blocked sessions, xmin horizon held (xid age — how far back the tool
# pins VACUUM). The probe connection (application_name='harness_probe') is
# excluded so gauges reflect the TOOL, not the probe.
GAUGE_FIELDS = ["longq", "longtxn", "conns", "locks", "blocked", "xmin_age"]


def gauge_once() -> tuple:
    try:
        v = _psql(ENG["gauge_sql"]).split(",")
        return (float(v[0]), float(v[1]), int(v[2]), int(v[3]), int(v[4]), int(float(v[5])))
    except (ValueError, IndexError, AttributeError):
        return (0.0, 0.0, 0, 0, 0, 0)


class GaugePoller(threading.Thread):
    """Samples the source gauges ~every 50 ms until stopped; reports peak + p50."""
    def __init__(self):
        super().__init__(daemon=True)
        self._halt = threading.Event()
        self.samples = []

    def run(self):
        while not self._halt.is_set():
            self.samples.append(gauge_once())
            self._halt.wait(0.05)

    def stop(self):
        self._halt.set()
        self.join(timeout=2)

    def result(self) -> dict:
        if not self.samples:
            return {f: 0 for f in GAUGE_FIELDS} | {"q_p50": 0.0}
        d = {f: max(s[i] for s in self.samples) for i, f in enumerate(GAUGE_FIELDS)}
        active = sorted(s[0] for s in self.samples if s[0] > 0)
        d["q_p50"] = active[len(active) // 2] if active else 0.0
        return d


class OltpProbe(threading.Thread):
    """Ground truth: a persistent client hammering point-reads on bench_probe
    while the tool runs. When the engine CLI exposes server-side timing (psql
    \\timing) it is parsed (DB latency, not docker); otherwise the round-trip is
    timed client-side — the idle baseline carries the same overhead, so the ratio
    still isolates the extract's impact. Harm = p99 during ÷ idle p99."""
    def __init__(self):
        super().__init__(daemon=True)
        self._halt = threading.Event()
        self.latencies = []
        self.proc = None

    def run(self):
        if ENG.get("probe_mode") == "perquery":
            self._run_perquery()
        else:
            self._run_persistent()

    def _run_perquery(self):
        """Fresh client per query, client-timed (docker overhead cancels in the
        ratio). Used where a persistent stdin protocol isn't reliable (mysql)."""
        i = 0
        while not self._halt.is_set():
            i = i % 1000 + 1
            cmd = [a.format(i=i) for a in ENG["probe_cmd"]]
            t0 = time.monotonic()
            subprocess.run(cmd, capture_output=True)
            self.latencies.append((time.monotonic() - t0) * 1000)

    def _run_persistent(self):
        """Persistent connection, server-side \\timing parsed (DB latency)."""
        self.proc = subprocess.Popen(
            ENG["probe_argv"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1)
        if ENG["probe_init"]:
            self.proc.stdin.write(ENG["probe_init"]); self.proc.stdin.flush()
        time_re = ENG["probe_time_re"]
        i = 0
        while not self._halt.is_set():
            i = i % 1000 + 1
            try:
                self.proc.stdin.write(ENG["probe_query"].format(i=i))
                self.proc.stdin.flush()
            except (BrokenPipeError, ValueError):
                break
            for line in self.proc.stdout:
                if re.search(time_re, line):
                    self.latencies.append(float(re.search(time_re, line).group(1)))
                    break

    def stop(self):
        self._halt.set()
        if self.proc:
            try:
                self.proc.stdin.close()
                self.proc.terminate()
            except Exception:  # noqa
                pass
        self.join(timeout=2)

    def p99(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        return s[min(len(s) - 1, int(len(s) * 0.99))]


# ─── Type fidelity ───────────────────────────────────────────────────────────
# Compare each tool's parquet column types against the SOURCE postgres types by
# semantic family. A mismatch is a real fidelity loss: jsonb flattened to a plain
# string (loses the JSON logical type a downstream reader needs), a naive
# timestamp promoted to timestamptz (the naive→instant ambiguity — a silent wall-
# clock shift), a decimal collapsed to a float (precision loss). Same run.
def family(t: str) -> str:
    t = t.upper()
    if "TIMESTAMP" in t and "WITH TIME ZONE" in t:
        return "timestamptz"
    if "TIMESTAMP" in t or t.startswith("DATETIME"):
        return "timestamp"
    if t == "DATE":
        return "date"
    if "JSON" in t:
        return "json"
    if "UUID" in t:
        return "uuid"
    if "BOOL" in t:
        return "bool"
    if "DECIMAL" in t or "NUMERIC" in t:
        return "decimal"
    if "DOUBLE" in t or "REAL" in t or "FLOAT" in t:
        return "float"
    if "INT" in t:  # BIGINT / INTEGER / SMALLINT / TINYINT / HUGEINT
        return "int"
    if "BLOB" in t or "BYTEA" in t or "BINARY" in t:
        return "binary"
    return "text"  # VARCHAR / TEXT / CHAR / …


def col_types(desc_sql: list) -> dict:
    d = {}
    for line in desc_sql:
        if "=" in line:
            k, v = line.split("=", 1)
            d[k] = v
    return d


def pg_types(table: str) -> dict:  # name kept; source-column types for the engine
    return col_types(_psql(ENG["types_sql"].format(t=table)).splitlines())


def parquet_families(path: Path) -> dict:
    """{column: semantic_family} for a tool's output parquet."""
    files = list(path.rglob("*.parquet")) if path.is_dir() else [path]
    if not files:
        return {}
    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"SELECT column_name||'='||column_type FROM "
            f"(DESCRIBE SELECT * FROM read_parquet('{files[0]}'))"])
    return {c: family(t) for c, t in col_types(r.stdout.strip().splitlines()).items()}


# Short labels for the type matrix so it fits a terminal.
FAM_ABBR = {"timestamp": "ts", "timestamptz": "tstz", "decimal": "dec", "binary": "bin"}
TOOL_ABBR = {"clickhouse-local": "click", "odbc2parquet": "odbc"}


def clean_stderr(err: str) -> str:
    """Drop the trailing /usr/bin/time -l rusage block so the tool's own error
    is what survives (time's lines are `<ws><number> <label>` or `... real`)."""
    keep = [ln for ln in err.splitlines()
            if not re.match(r"\s*[\d.]+\s+\w", ln) and ln.strip()]
    return "\n".join(keep)


def timed(cmd, cwd=None, env=None) -> tuple[float, float, int, str]:
    """Run `cmd` under /usr/bin/time -l. Returns (wall_s, peak_rss_mb, rc, stderr)."""
    wrapped = ["/usr/bin/time", "-l"] + cmd
    t0 = time.monotonic()
    p = subprocess.run(wrapped, cwd=cwd, env=env, capture_output=True, text=True)
    wall = time.monotonic() - t0
    m = re.search(r"(\d+)\s+maximum resident set size", p.stderr)
    rss_mb = (int(m.group(1)) / 1_048_576) if m else float("nan")
    return wall, rss_mb, p.returncode, clean_stderr(p.stderr)


# ─── Tool adapters ───────────────────────────────────────────────────────────
# Each returns (output_path, run_callable). run_callable() -> (wall, rss, rc, err).
# content_items has a BIGINT id PK, so numeric chunking / range reads apply.

def a_rivet(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    cfg = dest / "rivet.yaml"
    # Batch snapshot export: mode:full → one clean parquet file, streamed via a
    # server-side cursor (bounded memory). tuning.profile: fast drops the Balanced
    # default's 50 ms per-batch throttle + doubles the batch target — MEASURED
    # +24% rows/s, -13% source CPU, -12s open-txn, same 56 MB peak / 0 type-loss;
    # the only cost is oltp_p99 4.0→5.1x (no concurrent-load rate-limit). chunked
    # mode is the source-safety option for huge/resumable tables (separate).
    cfg.write_text(
        f"source:\n  type: {ENG['rivet_type']}\n  url: \"{ENG['url']('rivet')}\"\n"
        f"exports:\n  - name: {table}\n"
        f"    query: \"SELECT * FROM {table}\"\n"
        f"    mode: full\n"
        f"    format: parquet\n    compression: zstd\n"
        f"    tuning:\n      profile: fast\n"
        f"    destination:\n      type: local\n      path: {dest}\n"
    )
    return dest, lambda: timed(["rivet", "run", "-c", str(cfg)])


def a_rivet_chunked(table, dest: Path):
    """rivet in chunked mode (chunk_size 500k → ~4 files on 2M) — the source-
    safety scenario: a handful of bounded queries instead of one long scan, so
    the query hold is seconds not minutes, without fragmenting the output."""
    dest.mkdir(parents=True, exist_ok=True)
    cfg = dest / "rivet.yaml"
    cfg.write_text(
        f"source:\n  type: {ENG['rivet_type']}\n  url: \"{ENG['url']('rivet')}\"\n"
        f"exports:\n  - name: {table}\n"
        f"    query: \"SELECT * FROM {table}\"\n"
        f"    mode: chunked\n    chunk_column: id\n    chunk_size: 500000\n"
        f"    format: parquet\n    compression: zstd\n"
        f"    destination:\n      type: local\n      path: {dest}\n"
    )
    return dest, lambda: timed(["rivet", "run", "-c", str(cfg)])


def a_duckdb(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    sql = (
        f"{ENG['duckdb_install']} SET memory_limit='4GB'; "
        f"{ENG['duckdb_attach']}; "
        f"COPY (SELECT * FROM src.{table}) TO '{out}' (FORMAT parquet, COMPRESSION zstd);"
    )
    return out, lambda: timed(["duckdb", "-c", sql])


# clickhouse's brew cask is an unsigned Apple-Silicon binary; if `which` misses
# it (or Gatekeeper removed it) fall back to the homebrew path. Dequarantine +
# ad-hoc sign it once: `xattr -dr com.apple.quarantine <bin>; codesign -s - <bin>`.
CH_BIN = shutil.which("clickhouse") or "/opt/homebrew/bin/clickhouse"


def a_clickhouse(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    if out.exists():
        out.unlink()
    q = (f"SELECT * FROM {ENG['clickhouse_fn'](table)} "
         f"INTO OUTFILE '{out}' FORMAT Parquet")
    # steelman: cap memory to its lowest completing floor (OOMs below) — this is
    # the min-memory goal, and it LOWERS clickhouse's RSS (flatters it), not a
    # handicap. Threads left at default (all cores): capping them would only slow
    # it, which the min-memory goal doesn't require. CLI settings = --<name>=<val>.
    return out, lambda: timed([
        CH_BIN, "local", "--max_memory_usage=4000000000", "--query", q])


def a_odbc2parquet(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    conn = ENG["odbc_conn"]
    return out, lambda: timed([
        "odbc2parquet", "query", "--connection-string", conn,
        "--batch-size-memory", "256Mb", str(out), f"SELECT * FROM {table}"])


def a_sling(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    # sling has no native parquet writer — it streams source→CSV→local HTTP→duckdb
    # read_csv→parquet, which TIMES OUT on a single 2M-row stream. file_max_rows
    # rotates the output so each duckdb COPY is bounded — its steelman for scale.
    env = dict(os.environ, SMOKE_SRC=ENG["sling_url"])
    dest = out.parent / table  # a directory of rotated parts
    return dest, lambda: timed([
        "sling", "run",
        "--src-conn", "SMOKE_SRC", "--src-stream", ENG["qualify"](table),
        "--tgt-object", f"file://{dest}",
        "--tgt-options", '{"format": "parquet", "file_max_rows": 250000}'], env=env)


def a_ingestr(table, dest: Path):
    # ingestr 1.0.73+ writes a local parquet file via the parquet:// scheme.
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    return out, lambda: timed([
        "ingestr", "ingest",
        "--source-uri", ENG["ingestr_url"], "--source-table", ENG["qualify"](table),
        "--dest-uri", f"parquet://{out}", "--yes"])


def a_dlt(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    script = dest / "dlt_job.py"
    script.write_text(
        "import os, dlt\n"
        "from dlt.sources.sql_database import sql_table\n"
        f"os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '5000'\n"
        f"tbl = sql_table(credentials='{ENG['dlt_url']}', table='{table}', schema='{ENG['dlt_schema']}')\n"
        f"p = dlt.pipeline(pipeline_name='smoke_{table}', destination=dlt.destinations.filesystem('file://{dest}'), dataset_name='smoke')\n"
        "p.run(tbl, loader_file_format='parquet')\n"
    )
    # dlt + sqlalchemy live in the system python 3.9 (the homebrew pythons ship a
    # libexpat-mismatched pyexpat that breaks pip/import); use it explicitly.
    py = "/usr/bin/python3" if Path("/usr/bin/python3").exists() else sys.executable
    return dest, lambda: timed([py, str(script)], cwd=str(dest))


ADAPTERS = {
    "rivet": a_rivet, "rivet-chunked": a_rivet_chunked,
    "duckdb": a_duckdb, "clickhouse-local": a_clickhouse,
    "odbc2parquet": a_odbc2parquet, "sling": a_sling, "ingestr": a_ingestr,
    "dlt": a_dlt,
}


MATRIX_YAML = ROOT / "docs" / "bench" / "matrix.yaml"

# The catalog in matrix.yaml owns WHICH metrics, their labels, order, and harm
# grouping — one source. Only the presentation FORMAT is cosmetic and stays here.
FMT = {
    "oltp_x": lambda v: f"{v:.1f}x",
    "wall_s": lambda v: f"{v:.1f}", "rows_s": lambda v: f"{v:.0f}",
    "peak_rss_mb": lambda v: f"{v:.0f}", "out_mb": lambda v: f"{v:.1f}",
    "temp_mb": lambda v: f"{v:.1f}", "cache_mb": lambda v: f"{v:.0f}",
    "cpu_ms": lambda v: f"{v:.0f}",
    "longq": lambda v: f"{v:.2f}", "q_p50": lambda v: f"{v:.2f}",
    "longtxn": lambda v: f"{v:.2f}",
}


def fmt(key, v):
    f = FMT.get(key)
    if f:
        return f(v)
    if isinstance(v, float):  # counts: drop the trailing .0
        return f"{v:.0f}" if v == int(v) else f"{v:.1f}"
    return str(v)


def ta(t):
    return TOOL_ABBR.get(t, t)[:9]


def load_catalog() -> dict:
    try:
        import yaml
    except ModuleNotFoundError:
        sys.exit("smoke.py reads docs/bench/matrix.yaml and needs PyYAML — run it "
                 "with the system python that has it: /usr/bin/python3 dev/bench/smoke.py")
    return yaml.safe_load(MATRIX_YAML.read_text())


def main():
    global ENG
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="postgres", choices=list(ENGINES))
    ap.add_argument("--table", default="content_items")
    ap.add_argument("--tools", default=",".join(ADAPTERS))
    args = ap.parse_args()
    ENG = ENGINES[args.engine]

    catalog = load_catalog()
    bench_defs = catalog["benchmark_metrics"]
    # universal harm axes + THIS engine's native counters. Engines whose harm
    # entries lack label/group (mysql/mssql/mongo) default to key + a counters group.
    eng_harm = [{"key": d["key"], "label": d.get("label", d["key"]),
                 "group": d.get("group", f"{args.engine} counters")}
                for d in catalog["harm_metrics"].get(args.engine, [])]
    harm_defs = catalog["harm_metrics"]["universal"] + eng_harm
    declared = {d["key"] for d in bench_defs + harm_defs}

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    setup_harm()

    mt = re.search(r"\d+", _psql(f"SELECT COUNT(*) FROM {args.table}") or "")
    truth = int(mt.group()) if mt else -1
    src_types = pg_types(args.table)
    srcfam = {c: family(t) for c, t in src_types.items()}

    # Idle OLTP baseline (p99 of the probe with nothing else running).
    base = OltpProbe(); base.start(); time.sleep(1.5); base.stop()
    base_p99 = base.p99() or 1e-9
    print(f"\n{args.engine}.{args.table}  (source rows={truth}, {len(src_types)} cols, "
          f"OLTP baseline p99={base_p99:.2f}ms)")

    results = []  # (tool, m_dict, pqfam)
    for tool in args.tools.split(","):
        tool = tool.strip()
        if tool not in ADAPTERS:
            continue
        if tool == "clickhouse-local":
            available = Path(CH_BIN).exists()
        elif tool == "dlt":
            available = True
        elif tool == "rivet-chunked":
            available = shutil.which("rivet") is not None
        else:
            available = shutil.which(tool) is not None
        if not available:
            print(f"  {tool:<16} missing"); continue

        path, run = ADAPTERS[tool](args.table, OUT / tool)
        c0, cpu0, dead0 = counters(), cgroup_cpu_usec(), dead_tuples(args.table)
        poller, probe = GaugePoller(), OltpProbe()
        poller.start(); probe.start()
        try:
            wall, rss, rc, err = run()
        except Exception as e:  # noqa
            poller.stop(); probe.stop()
            print(f"  {tool:<16} error: {str(e)[:50]}"); continue
        poller.stop(); probe.stop()
        time.sleep(0.4)  # let the stats collector flush this tool's reads
        c1, cpu1 = counters(), cgroup_cpu_usec()

        g = poller.result()
        rows = parquet_rows(path) if rc == 0 else -1
        mb = out_bytes(path) / 1_048_576 if rc == 0 else 0
        pqfam = parquet_families(path) if rc == 0 else {}
        status = "ok" if rc == 0 and rows == truth else ("mismatch" if rc == 0 else "error")
        # One value dict keyed by the matrix.yaml metric keys (bench + harm).
        m = {
            "status": status, "wall_s": wall, "rows": rows,
            "rows_s": (rows / wall) if wall > 0 and rows > 0 else 0,
            "peak_rss_mb": rss, "out_mb": mb,
            "out_files": len(list((OUT / tool).rglob("*.parquet"))) if rc == 0 else 0,
            "type_deg": sum(1 for c in src_types
                            if c in pqfam and pqfam[c] != srcfam[c]),
            "oltp_x": (probe.p99() / base_p99) if probe.p99() else 0.0,
            "xmin_age": g["xmin_age"], "dead": dead_tuples(args.table) - dead0,
            "cache_mb": cache_footprint_mb(args.table),
            "locks": g["locks"], "blocked": g["blocked"],
            "longq": g["longq"], "q_p50": g["q_p50"], "longtxn": g["longtxn"],
            "conns": g["conns"], "cpu_ms": (cpu1 - cpu0) / 1000,
        }
        # Engine-native counter deltas keyed to matrix.yaml harm_metrics.<engine>.
        if ENG["name"] == "postgres":
            m.update({
                "tup_returned": c1.get("tup_returned", 0) - c0.get("tup_returned", 0),
                "tup_fetched": c1.get("tup_fetched", 0) - c0.get("tup_fetched", 0),
                "temp_mb": (c1.get("temp_bytes", 0) - c0.get("temp_bytes", 0)) / 1e6,
                "blks_read": c1.get("blks_read", 0) - c0.get("blks_read", 0),
                "blk_read_time": c1.get("blk_read_time", 0) - c0.get("blk_read_time", 0),
                "wal_kb": (c1.get("wal_bytes", 0) - c0.get("wal_bytes", 0)) / 1024,
            })
        else:  # mysql/mssql/mongo: raw native counter deltas
            for k in ENG["counter_keys"]:
                m[k] = c1.get(k, 0) - c0.get(k, 0)
        # Anti-drift guard: every metric matrix.yaml declares must be captured.
        gap = declared - set(m)
        if gap:
            sys.exit(f"metric drift: matrix.yaml declares {sorted(gap)} with no "
                     f"capture in smoke.py — reconcile the two.")
        results.append((tool, m, pqfam))
        tail = "" if rc == 0 else " · " + (err.strip().splitlines() or ["?"])[-1][:60]
        print(f"  {tool:<16} {status:<9} wall={wall:5.1f}s  oltp={m['oltp_x']:.1f}x{tail}")

    ok = [r for r in results if r[1]["status"] != "error"]

    # ── Matrix 1 — Benchmark (from catalog: benchmark_metrics) ────────────────
    print("\n### 1. benchmark — throughput & correctness\n")
    widths = [18] + [max(len(d["label"]) + 2, 9) for d in bench_defs]
    head = f"{'tool':<18}" + "".join(f"{d['label']:>{w}}"
                                     for d, w in zip(bench_defs, widths[1:]))
    print(head); print("-" * len(head))
    for tool, m, _pf in results:
        line = f"{tool:<18}" + "".join(f"{fmt(d['key'], m[d['key']]):>{w}}"
                                       for d, w in zip(bench_defs, widths[1:]))
        print(line)

    # ── Matrix 2 — Harm (from catalog: harm_metrics, grouped by surface) ──────
    if ok:
        print("\n### 2. harm to the source — how much each tool hurts the DB\n")
        w0, wt = 16, 9
        print(f"{'harm metric':<{w0}}" + "".join(f"{ta(t):>{wt}}" for t, *_ in ok))
        print("-" * (w0 + wt * len(ok)))
        # Group by blast surface — merge same-group metrics across the
        # universal/per-engine split so each surface prints once, contiguously.
        order, by_group = [], {}
        for d in harm_defs:
            grp = d.get("group", "")
            if grp not in by_group:
                by_group[grp] = []; order.append(grp)
            by_group[grp].append(d)
        for grp in order:
            if grp:
                print(f"— {grp} —")
            for d in by_group[grp]:
                line = f"{d['label']:<{w0}}"
                for _t, m, _pf in ok:
                    line += f"{fmt(d['key'], m[d['key']]):>{wt}}"
                print(line)

    # ── Matrix 3 — Type loss (source column × tool parquet family) ────────────
    fam_by_tool = [(t, pf) for t, _m, pf in ok]
    disc = [c for c in src_types
            if any(pf.get(c, srcfam[c]) != srcfam[c] for _, pf in fam_by_tool)]
    if disc and fam_by_tool:
        print("\n### 3. type loss — source vs each tool's parquet family (* = drift)\n")
        tcols = [("column", 16, "<"), ("source", 8, "<")] + \
                [(ta(t), 10, "<") for t, _ in fam_by_tool]
        print("".join(f"{n:{a}{w}}" for n, w, a in tcols))
        for c in disc:
            cells = [c, FAM_ABBR.get(srcfam[c], srcfam[c])]
            for _, pf in fam_by_tool:
                v = pf.get(c)
                cells.append(FAM_ABBR.get(v, v) + ("*" if v != srcfam[c] else "") if v else "-")
            print("".join(f"{str(v):{a}{w}}" for v, (n, w, a) in zip(cells, tcols)))
    print()


if __name__ == "__main__":
    main()
