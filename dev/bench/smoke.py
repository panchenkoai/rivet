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


def a_clickhouse(table, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"{table}.parquet"
    if out.exists():
        out.unlink()
    q = (f"SELECT * FROM postgresql('{PG['host']}:{PG['port']}', '{PG['db']}', "
         f"'{table}', '{PG['user']}', '{PG['pw']}') "
         f"INTO OUTFILE '{out}' FORMAT Parquet")
    return out, lambda: timed(["clickhouse", "local", "--query", q])


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

    print(f"\nSmoke: postgres.{args.table}  (source rows = {truth})")
    print("perf (wall/peak_rss/out) + source-harm (tup_read/temp/blks) — one run each\n")
    hdr = (f"{'tool':<18}{'status':<9}{'wall_s':>8}{'peak_mb':>9}{'rows':>8}{'out_mb':>8}"
           f"{'tup_read':>10}{'temp_mb':>9}{'blks_rd':>9}")
    print(hdr); print("-" * len(hdr))

    rows_out = []
    for tool in args.tools.split(","):
        tool = tool.strip()
        if tool not in ADAPTERS:
            continue
        # dlt runs via `python -m` (no `dlt` binary needed); the rest need a bin.
        probe = {"clickhouse-local": "clickhouse", "dlt": None}.get(tool, tool)
        if probe and shutil.which(probe) is None:
            print(f"{tool:<18}{'missing':<9}"); continue
        path, run = ADAPTERS[tool](args.table, OUT / tool)
        before = pg_harm()
        try:
            wall, rss, rc, err = run()
        except Exception as e:  # noqa
            print(f"{tool:<18}{'error':<9}  {str(e)[:50]}"); continue
        time.sleep(0.4)  # let the stats collector flush this tool's reads
        harm = {k: pg_harm().get(k, 0) - before.get(k, 0) for k in HARM_KEYS}
        rows = parquet_rows(path) if rc == 0 else -1
        mb = out_bytes(path) / 1_048_576 if rc == 0 else 0
        status = "ok" if rc == 0 and rows == truth else ("mismatch" if rc == 0 else "error")
        print(f"{tool:<18}{status:<9}{wall:>8.1f}{rss:>9.0f}{rows:>8}{mb:>8.1f}"
              f"{harm['tup_returned']:>10.0f}{harm['temp_bytes']/1e6:>9.1f}"
              f"{harm['blks_read']:>9.0f}")
        if rc != 0:
            tail = (err.strip().splitlines() or ["(no stderr)"])[-1][:90]
            print(f"{'':<18}└─ {tail}")
        rows_out.append((tool, status, wall, rss, rows, mb, harm))

    print()


if __name__ == "__main__":
    main()
