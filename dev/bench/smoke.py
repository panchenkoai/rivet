#!/usr/bin/env python3
"""Benchmark smoke runner — reads docs/bench/matrix.yaml, runs each tool over the
seeded smoke fixture, and prints a comparison table.

Smokes first: a tiny fixture on one DB, every tool, so a full round-trip is a
couple of minutes. A tool that errors is a RESULT (status=error), not a stop —
the harness records it and moves on.

    python3 dev/bench/smoke.py                 # postgres content_items, all tools
    python3 dev/bench/smoke.py --table orders  # a different smoke table

Measures per run: wall seconds, peak RSS (MB), output rows, output bytes.
Peak RSS comes from `/usr/bin/time -l` (macOS, bytes) so it isolates the tool
process; rows are counted from the emitted parquet via the duckdb CLI.
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


# ─── Source-harm counters ────────────────────────────────────────────────────
# Sampled before → after each tool's run. Tools run SERIALLY, so the delta is
# attributable to that one tool. pg_stat_database is cumulative and db-wide; the
# delta is the read amplification (tup_returned), temp-file spill (temp_bytes)
# and disk reads (blks_read) a tool inflicts — captured in the SAME run as
# wall/RSS, which is the whole point (harm + perf, one pass).
HARM_KEYS = ["tup_returned", "tup_fetched", "temp_bytes", "blks_read", "blk_read_time"]
PG_CONTAINER = "rivet-mongo-postgres-1"


def pg_harm() -> dict:
    q = ("SELECT tup_returned||','||tup_fetched||','||temp_bytes||','||blks_read"
         f"||','||blk_read_time FROM pg_stat_database WHERE datname='{PG['db']}'")
    r = sh(["docker", "exec", PG_CONTAINER, "psql", "-U", PG["user"],
            "-d", PG["db"], "-tAc", q])
    try:
        return dict(zip(HARM_KEYS, [float(v) for v in r.stdout.strip().split(",")]))
    except (ValueError, AttributeError):
        return {}


# ─── Point-in-time harm gauges (the old harm_ab.sh signals) ──────────────────
# The cumulative counters above are before/after deltas; the gauges below —
# longest running query, longest open transaction, held locks, live connections
# — are instantaneous, so they need a background sampler that records the PEAK
# during the tool's run (harm_ab.sh polled @50ms; same idea). These are the
# source-safety headlines: a chunked reader holds short queries and few locks; a
# `SELECT *` that buffers holds one long query and pins a transaction.
GAUGE_SQL = (
    "SELECT COALESCE(EXTRACT(EPOCH FROM "
    "max(now()-query_start) FILTER (WHERE state='active')),0)||','||"
    "COALESCE(EXTRACT(EPOCH FROM max(now()-xact_start)),0)||','||count(*)||','||"
    "(SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a2 ON l.pid=a2.pid "
    f"WHERE a2.datname='{PG['db']}' AND a2.pid<>pg_backend_pid()) "
    f"FROM pg_stat_activity WHERE datname='{PG['db']}' "
    "AND pid<>pg_backend_pid() AND backend_type='client backend'"
)


def gauge_once() -> tuple:
    r = sh(["docker", "exec", PG_CONTAINER, "psql", "-U", PG["user"],
            "-d", PG["db"], "-tAc", GAUGE_SQL])
    try:
        lq, lx, cn, lk = r.stdout.strip().split(",")
        return (float(lq), float(lx), int(cn), int(lk))
    except (ValueError, AttributeError):
        return (0.0, 0.0, 0, 0)


class GaugePoller(threading.Thread):
    """Samples the source gauges ~every 50 ms until stopped; reports the peak."""
    def __init__(self):
        super().__init__(daemon=True)
        self._stop = threading.Event()
        self.samples = []

    def run(self):
        while not self._stop.is_set():
            self.samples.append(gauge_once())
            self._stop.wait(0.05)

    def stop(self):
        self._stop.set()
        self.join(timeout=2)

    def peak(self) -> tuple:
        if not self.samples:
            return (0.0, 0.0, 0, 0)
        return tuple(max(s[i] for s in self.samples) for i in range(4))


def dead_tuples(table: str) -> int:
    r = sh(["docker", "exec", PG_CONTAINER, "psql", "-U", PG["user"], "-d", PG["db"],
            "-tAc", f"SELECT COALESCE(n_dead_tup,0) FROM pg_stat_user_tables "
                    f"WHERE relname='{table}'"])
    try:
        return int(r.stdout.strip() or 0)
    except ValueError:
        return 0


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
    return out, lambda: timed([CH_BIN, "local", "--query", q])


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="content_items")
    ap.add_argument("--tools", default=",".join(ADAPTERS))
    args = ap.parse_args()

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    src_rows = parquet_rows  # noqa (placeholder to keep name)
    # source truth for the correctness gate
    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"ATTACH 'host={PG['host']} port={PG['port']} dbname={PG['db']} user={PG['user']} password={PG['pw']}' AS pg (TYPE postgres, READ_ONLY); SELECT count(*) FROM pg.{args.table};"])
    m = re.search(r"\d+", r.stdout or "")
    truth = int(m.group()) if m else -1

    src_types = pg_types(args.table)

    # One matrix, every comparison axis as a column:
    #   correctness (status/rows) · perf (wall/peak_rss/out) ·
    #   source-harm (tup_read/temp/blks) · type fidelity (type_deg)
    srcfam = {c: family(t) for c, t in src_types.items()}

    # One matrix, every comparison axis as a column:
    #   correctness (status/rows) · perf (wall/peak_rss/out) ·
    #   source-harm cumulative (tup_read/temp) + gauge peaks (longq_s/locks/dead) ·
    #   type fidelity (type_deg).
    print(f"\nSmoke: postgres.{args.table}  (source rows = {truth}, {len(src_types)} cols)")
    print("all axes, one run each — correctness · perf · source-harm · type fidelity\n")
    cols = [("tool", 18, "<"), ("status", 9, "<"), ("wall_s", 8, ">"),
            ("peak_mb", 9, ">"), ("rows", 7, ">"), ("out_mb", 8, ">"),
            ("tup_read", 9, ">"), ("temp_mb", 8, ">"), ("longq_s", 9, ">"),
            ("locks", 7, ">"), ("dead", 7, ">"), ("type_deg", 9, ">")]
    hdr = "".join(f"{n:{a}{w}}" for n, w, a in cols)

    def row(vals):
        return "".join(f"{str(v):{a}{w}}" for v, (n, w, a) in zip(vals, cols))

    print(hdr); print("-" * len(hdr))

    fam_by_tool = []
    for tool in args.tools.split(","):
        tool = tool.strip()
        if tool not in ADAPTERS:
            continue
        # dlt runs via `python -m` (no `dlt` binary needed); clickhouse resolves
        # to CH_BIN (may be an absolute path); the rest need a bin on PATH.
        if tool == "clickhouse-local":
            available = Path(CH_BIN).exists()
        elif tool == "dlt":
            available = True
        else:
            available = shutil.which(tool) is not None
        if not available:
            print(f"{tool:<18}{'missing':<9}"); continue
        path, run = ADAPTERS[tool](args.table, OUT / tool)
        dead0 = dead_tuples(args.table)
        before = pg_harm()
        poller = GaugePoller(); poller.start()
        try:
            wall, rss, rc, err = run()
        except Exception as e:  # noqa
            poller.stop(); print(f"{tool:<18}{'error':<9}  {str(e)[:50]}"); continue
        poller.stop()
        time.sleep(0.4)  # let the stats collector flush this tool's reads
        harm = {k: pg_harm().get(k, 0) - before.get(k, 0) for k in HARM_KEYS}
        longq, _longx, _conns, locks = poller.peak()
        dead = dead_tuples(args.table) - dead0
        rows = parquet_rows(path) if rc == 0 else -1
        mb = out_bytes(path) / 1_048_576 if rc == 0 else 0
        pqfam = parquet_families(path) if rc == 0 else {}
        degr = [(c, srcfam[c], pqfam[c]) for c in src_types
                if c in pqfam and pqfam[c] != srcfam[c]]
        status = "ok" if rc == 0 and rows == truth else ("mismatch" if rc == 0 else "error")
        print(row([tool, status, f"{wall:.1f}", f"{rss:.0f}", rows, f"{mb:.1f}",
                   f"{harm['tup_returned']:.0f}", f"{harm['temp_bytes'] / 1e6:.1f}",
                   f"{longq:.2f}", locks, dead, len(degr)]))
        if rc != 0:
            tail = (err.strip().splitlines() or ["(no stderr)"])[-1][:90]
            print(f"{'':<18}└─ {tail}")
        if rc == 0:
            fam_by_tool.append((tool, pqfam))

    # ── Second matrix: which type degraded, per column × tool ────────────────
    disc = [c for c in src_types
            if any(pf.get(c, srcfam[c]) != srcfam[c] for _, pf in fam_by_tool)]
    if disc and fam_by_tool:
        def fa(f):
            return FAM_ABBR.get(f, f)

        def ta(t):
            return TOOL_ABBR.get(t, t)[:9]
        print("\ntype fidelity — source vs each tool's parquet family (* = drift):\n")
        tcols = [("column", 16, "<"), ("source", 8, "<")] + \
                [(ta(t), 10, "<") for t, _ in fam_by_tool]
        print("".join(f"{n:{a}{w}}" for n, w, a in tcols))
        for c in disc:
            cells = [c, fa(srcfam[c])]
            for _, pf in fam_by_tool:
                v = pf.get(c)
                cells.append(fa(v) + ("*" if v != srcfam[c] else "") if v else "-")
            print("".join(f"{str(v):{a}{w}}" for v, (n, w, a) in zip(cells, tcols)))
    print()


if __name__ == "__main__":
    main()
