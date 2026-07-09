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

# Smoke fixture lives in a dedicated DB so the live-test `rivet` fixture is never
# touched (see the seed step). Host → docker-mapped ports.
PG = {
    "host": "localhost", "port": "5432", "db": "rivet_bench",
    "user": "rivet", "pw": "rivet",
}
PG_URL = f"postgresql://{PG['user']}:{PG['pw']}@{PG['host']}:{PG['port']}/{PG['db']}"
# Docker postgres has no TLS; sling/ingestr default to sslmode=require and must
# be told to disable it (rivet/duckdb/odbc2parquet don't force TLS on loopback).
PG_URL_NOSSL = PG_URL + "?sslmode=disable"


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
PG_CONTAINER = "rivet-mongo-postgres-1"
HARM_KEYS = ["tup_returned", "tup_fetched", "temp_bytes", "blks_read", "blk_read_time"]


def _psql(q: str) -> str:
    return sh(["docker", "exec", PG_CONTAINER, "psql", "-U", PG["user"],
               "-d", PG["db"], "-tAc", q]).stdout.strip()


def setup_harm():
    """Idempotent prerequisites: buffer-cache view + OLTP probe table."""
    _psql("CREATE EXTENSION IF NOT EXISTS pg_buffercache")
    _psql("CREATE TABLE IF NOT EXISTS bench_probe(id int PRIMARY KEY, v int)")
    if _psql("SELECT count(*) FROM bench_probe") in ("", "0"):
        _psql("INSERT INTO bench_probe SELECT g, g FROM generate_series(1,1000) g")


def pg_counters() -> dict:
    """Cumulative counters for before/after deltas: pg_stat_database + WAL lsn."""
    q = ("SELECT tup_returned||','||tup_fetched||','||temp_bytes||','||blks_read"
         "||','||blk_read_time||','||pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0') "
         f"FROM pg_stat_database WHERE datname='{PG['db']}'")
    try:
        return dict(zip(HARM_KEYS + ["wal_bytes"], [float(v) for v in _psql(q).split(",")]))
    except (ValueError, AttributeError):
        return {}


def cgroup_cpu_usec() -> int:
    """Source-container CPU microseconds consumed (cgroup v2 cpu.stat)."""
    r = sh(["docker", "exec", PG_CONTAINER, "sh", "-c",
            "awk '/usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat"])
    try:
        return int(r.stdout.strip() or 0)
    except ValueError:
        return 0


def cache_footprint_mb(table: str) -> float:
    """MB of shared_buffers holding the table's pages after the run — the cache
    the extract polluted (pages that displaced the hot OLTP working set)."""
    n = _psql("SELECT count(*) FROM pg_buffercache b JOIN pg_class c "
              "ON b.relfilenode = pg_relation_filenode(c.oid) "
              f"WHERE c.relname='{table}'")
    try:
        return int(n) * 8 / 1024
    except ValueError:
        return 0.0


def dead_tuples(table: str) -> int:
    n = _psql(f"SELECT COALESCE(n_dead_tup,0) FROM pg_stat_user_tables WHERE relname='{table}'")
    try:
        return int(n or 0)
    except ValueError:
        return 0


# ── Point-in-time gauges (peak + p50 over a background poller) ───────────────
# Each sample: longest active query (s), longest open txn (s), connections, held
# locks, blocked sessions, xmin horizon held (xid age — how far back the tool
# pins VACUUM). The probe connection (application_name='harness_probe') is
# excluded so gauges reflect the TOOL, not the probe.
GAUGE_FIELDS = ["longq", "longtxn", "conns", "locks", "blocked", "xmin_age"]
GAUGE_SQL = (
    "SELECT COALESCE(EXTRACT(EPOCH FROM max(now()-query_start) "
    "FILTER (WHERE state='active')),0)||','||"
    "COALESCE(EXTRACT(EPOCH FROM max(now()-xact_start)),0)||','||count(*)||','||"
    "(SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a2 ON l.pid=a2.pid "
    f"WHERE a2.datname='{PG['db']}' AND a2.pid<>pg_backend_pid() "
    "AND a2.application_name<>'harness_probe')||','||"
    f"(SELECT count(*) FROM pg_stat_activity a3 WHERE a3.datname='{PG['db']}' "
    "AND cardinality(pg_blocking_pids(a3.pid))>0)||','||"
    "COALESCE(max(age(backend_xmin)),0) "
    f"FROM pg_stat_activity WHERE datname='{PG['db']}' AND pid<>pg_backend_pid() "
    "AND backend_type='client backend' AND application_name<>'harness_probe'"
)


def gauge_once() -> tuple:
    try:
        v = _psql(GAUGE_SQL).split(",")
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
    """Ground truth: a persistent connection hammering point-reads on bench_probe
    while the tool runs. Latency is server-side (psql \\timing), so it is DB
    latency, not docker/network. Harm = p99 during the extract ÷ idle p99."""
    def __init__(self):
        super().__init__(daemon=True)
        self._halt = threading.Event()
        self.latencies = []
        self.proc = None

    def run(self):
        self.proc = subprocess.Popen(
            ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG["user"],
             "-d", PG["db"], "-qAt"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1)
        self.proc.stdin.write("SET application_name='harness_probe';\n\\timing on\n")
        self.proc.stdin.flush()
        i = 0
        while not self._halt.is_set():
            i = i % 1000 + 1
            try:
                self.proc.stdin.write(f"SELECT v FROM bench_probe WHERE id={i};\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, ValueError):
                break
            for line in self.proc.stdout:
                m = re.search(r"Time:\s+([\d.]+)\s+ms", line)
                if m:
                    self.latencies.append(float(m.group(1)))
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


def pg_types(table: str) -> dict:
    q = (f"SELECT column_name||'='||data_type FROM information_schema.columns "
         f"WHERE table_name='{table}' ORDER BY ordinal_position")
    r = sh(["docker", "exec", PG_CONTAINER, "psql", "-U", PG["user"],
            "-d", PG["db"], "-tAc", q])
    return col_types(r.stdout.strip().splitlines())


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
    cfg.write_text(
        f"source:\n  type: postgres\n  url: \"{PG_URL}\"\n"
        f"exports:\n  - name: {table}\n"
        f"    query: \"SELECT * FROM {table}\"\n"
        f"    mode: chunked\n    chunk_column: id\n    chunk_size: 25000\n"
        f"    format: parquet\n    compression: zstd\n"
        f"    destination:\n      type: local\n      path: {dest}\n"
    )
    return dest, lambda: timed(["rivet", "run", "-c", str(cfg)])


def a_duckdb(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    sql = (
        "INSTALL postgres; LOAD postgres; SET memory_limit='4GB'; "
        f"ATTACH 'host={PG['host']} port={PG['port']} dbname={PG['db']} "
        f"user={PG['user']} password={PG['pw']}' AS pg (TYPE postgres, READ_ONLY); "
        f"COPY (SELECT * FROM pg.{table}) TO '{out}' (FORMAT parquet, COMPRESSION zstd);"
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
    q = (f"SELECT * FROM postgresql('{PG['host']}:{PG['port']}', '{PG['db']}', "
         f"'{table}', '{PG['user']}', '{PG['pw']}') "
         f"INTO OUTFILE '{out}' FORMAT Parquet")
    # steelman: cap memory (OOMs below its floor) + bound threads — passed as CLI
    # settings (clickhouse-local accepts any setting as --<name>=<value>).
    return out, lambda: timed([
        CH_BIN, "local", "--max_memory_usage=4000000000", "--max_threads=4",
        "--query", q])


def a_odbc2parquet(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    conn = (f"Driver={{PostgreSQL Unicode}};Server={PG['host']};Port={PG['port']};"
            f"Database={PG['db']};Uid={PG['user']};Pwd={PG['pw']};")
    return out, lambda: timed([
        "odbc2parquet", "query", "--connection-string", conn,
        "--batch-size-memory", "256Mb", str(out), f"SELECT * FROM {table}"])


def a_sling(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    env = dict(os.environ, SMOKE_PG=PG_URL_NOSSL)
    return out, lambda: timed([
        "sling", "run",
        "--src-conn", "SMOKE_PG", "--src-stream", f"public.{table}",
        "--tgt-object", f"file://{out}",
        "--tgt-options", '{"format": "parquet"}'], env=env)


def a_ingestr(table, dest: Path):
    # ingestr 1.0.73+ writes a local parquet file via the parquet:// scheme.
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    return out, lambda: timed([
        "ingestr", "ingest",
        "--source-uri", PG_URL_NOSSL, "--source-table", f"public.{table}",
        "--dest-uri", f"parquet://{out}", "--yes"])


def a_dlt(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    script = dest / "dlt_job.py"
    script.write_text(
        "import os, dlt\n"
        "from dlt.sources.sql_database import sql_table\n"
        f"os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '5000'\n"
        f"tbl = sql_table(credentials='{PG_URL}', table='{table}', schema='public')\n"
        f"p = dlt.pipeline(pipeline_name='smoke_{table}', destination=dlt.destinations.filesystem('file://{dest}'), dataset_name='smoke')\n"
        "p.run(tbl, loader_file_format='parquet')\n"
    )
    # dlt + sqlalchemy live in the system python 3.9 (the homebrew pythons ship a
    # libexpat-mismatched pyexpat that breaks pip/import); use it explicitly.
    py = "/usr/bin/python3" if Path("/usr/bin/python3").exists() else sys.executable
    return dest, lambda: timed([py, str(script)], cwd=str(dest))


ADAPTERS = {
    "rivet": a_rivet, "duckdb": a_duckdb, "clickhouse-local": a_clickhouse,
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="content_items")
    ap.add_argument("--tools", default=",".join(ADAPTERS))
    args = ap.parse_args()

    catalog = load_catalog()
    bench_defs = catalog["benchmark_metrics"]
    harm_defs = catalog["harm_metrics"]["universal"] + catalog["harm_metrics"]["postgres"]
    declared = {d["key"] for d in bench_defs + harm_defs}

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    setup_harm()

    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"ATTACH 'host={PG['host']} port={PG['port']} dbname={PG['db']} "
            f"user={PG['user']} password={PG['pw']}' AS pg (TYPE postgres, READ_ONLY); "
            f"SELECT count(*) FROM pg.{args.table};"])
    m = re.search(r"\d+", r.stdout or "")
    truth = int(m.group()) if m else -1
    src_types = pg_types(args.table)
    srcfam = {c: family(t) for c, t in src_types.items()}

    # Idle OLTP baseline (p99 of the probe with nothing else running).
    base = OltpProbe(); base.start(); time.sleep(1.5); base.stop()
    base_p99 = base.p99() or 1e-9
    print(f"\nSmoke: postgres.{args.table}  (source rows={truth}, {len(src_types)} cols, "
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
        else:
            available = shutil.which(tool) is not None
        if not available:
            print(f"  {tool:<16} missing"); continue

        path, run = ADAPTERS[tool](args.table, OUT / tool)
        c0, cpu0, dead0 = pg_counters(), cgroup_cpu_usec(), dead_tuples(args.table)
        poller, probe = GaugePoller(), OltpProbe()
        poller.start(); probe.start()
        try:
            wall, rss, rc, err = run()
        except Exception as e:  # noqa
            poller.stop(); probe.stop()
            print(f"  {tool:<16} error: {str(e)[:50]}"); continue
        poller.stop(); probe.stop()
        time.sleep(0.4)  # let the stats collector flush this tool's reads
        c1, cpu1 = pg_counters(), cgroup_cpu_usec()

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
            "conns": g["conns"],
            "tup_returned": c1.get("tup_returned", 0) - c0.get("tup_returned", 0),
            "tup_fetched": c1.get("tup_fetched", 0) - c0.get("tup_fetched", 0),
            "temp_mb": (c1.get("temp_bytes", 0) - c0.get("temp_bytes", 0)) / 1e6,
            "blks_read": c1.get("blks_read", 0) - c0.get("blks_read", 0),
            "blk_read_time": c1.get("blk_read_time", 0) - c0.get("blk_read_time", 0),
            "wal_kb": (c1.get("wal_bytes", 0) - c0.get("wal_bytes", 0)) / 1024,
            "cpu_ms": (cpu1 - cpu0) / 1000,
        }
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
