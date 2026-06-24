#!/usr/bin/env python3
"""Cross-tool DATABASE-HARM matrix for PostgreSQL — the source-safety axis.

Throughput/RSS measure the TOOL. This measures what the tool does to the SOURCE
while it extracts the wide `content_items` fixture: how long it pins a snapshot,
how many backends it opens, whether it spills the source to temp, how much it
evicts the buffer pool. Most ingestion benchmarks (ingestr's included) never look
at this — it's exactly where a chunked, budgeted reader pulls ahead of a single
`SELECT *`.

For each tool on PATH it samples `pg_stat_activity` every 250 ms while the tool
runs (peak active backends, longest single in-flight query), and deltas the
cumulative `pg_stat_database` counters (temp bytes spilled, disk blocks read),
then prints a tool × harm-metric matrix.

The headline metric is **longest query (s)**: a `SELECT *` tool holds ONE snapshot
for the whole extraction (a transaction open the entire run — blocks VACUUM, pins
dead tuples); rivet's chunked keyset issues many short queries, so its longest is
a single chunk, not the whole run.

Usage:  python3 docs/bench/harness/pg_harm_matrix.py [table]
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PG_CONTAINER = os.environ.get("PG_CONTAINER", "rivet-postgres-1")
PG_URL = "postgresql://rivet:rivet@localhost:5432/rivet"
RIVET = os.environ.get(
    "RIVET_BIN", "/tmp/rivet_release/rivet-v0.14.0-aarch64-apple-darwin/rivet"
)
TABLE = sys.argv[1] if len(sys.argv) > 1 else "content_items"
# STEELMAN=1 gives each SELECT-* tool its best fetch config. The point is to PROVE
# the snapshot-hold is structural: these knobs tune the client-side fetch buffer
# (page/batch size), not the source query — so the hold should NOT move. Only rivet
# (keyset) and duckdb (ctid-parallel) actually chunk the source into many queries.
STEELMAN = bool(os.environ.get("STEELMAN"))


def pg(sql):
    out = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "rivet", "-d", "rivet", "-tAc", sql],
        capture_output=True, text=True,
    )
    return out.stdout.strip()


def counters():
    """(temp_bytes, blocks_accessed) for the bench DB — delta'd across a run. Blocks
    accessed = blks_read + blks_hit, so it counts every buffer touch independent of
    cache warmth (disk-only blks_read is confounded by run order — whoever runs first
    pays the cold reads and warms the cache for the rest)."""
    row = pg("SELECT temp_bytes, blks_read + blks_hit FROM pg_stat_database WHERE datname='rivet'")
    tb, ba = (row.split("|") + ["0", "0"])[:2]
    return int(tb or 0), int(ba or 0)


class Sampler(threading.Thread):
    """Poll pg_stat_activity every 250ms; keep the peak backends + longest query."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = False
        self.peak_backends = 0
        self.longest_query_s = 0.0

    def run(self):
        q = (
            "SELECT count(*), "
            "coalesce(max(extract(epoch from clock_timestamp()-query_start)),0) "
            "FROM pg_stat_activity WHERE datname='rivet' AND state='active' "
            "AND pid<>pg_backend_pid() AND query NOT LIKE '%pg_stat_activity%'"
        )
        while not self.stop:
            row = pg(q)
            if "|" in row:
                n, longest = row.split("|")
                self.peak_backends = max(self.peak_backends, int(n or 0))
                self.longest_query_s = max(self.longest_query_s, float(longest or 0))
            time.sleep(0.25)


def rivet_cmd(work):
    """rivet in CHUNKED mode (byte budget) — the gentle, recommended source path."""
    cfg = os.path.join(work, "rivet.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{ type: postgres, url: "{PG_URL}" }}\n'
            f"exports:\n"
            f"  - name: {TABLE}\n"
            f"    table: public.{TABLE}\n"
            f"    mode: chunked\n"
            f"    chunk_column: id\n"
            f"    chunk_size_memory_mb: 256\n"
            f"    format: parquet\n"
            f"    compression: snappy\n"
            f'    destination: {{ type: local, path: "{work}/rivet_out" }}\n'
        )
    return [RIVET, "run", "--config", cfg]


def sling_cmd(work):
    os.environ["PG_LOCAL"] = "postgres://rivet:rivet@localhost:5432/rivet?sslmode=disable"
    return [
        "sling", "run", "--src-conn", "PG_LOCAL", "--src-stream", f"public.{TABLE}",
        "--tgt-object", f"file://{work}/sling.parquet",
        "--tgt-options", "{format: parquet, compression: snappy}", "--mode", "full-refresh",
    ]


def duckdb_cmd(work):
    return ["duckdb", "-c",
        "INSTALL postgres; LOAD postgres; "
        "ATTACH 'host=localhost port=5432 user=rivet password=rivet dbname=rivet' AS pg (TYPE postgres, READ_ONLY); "
        f"COPY (SELECT * FROM pg.public.\"{TABLE}\") TO '{work}/duck.parquet' (FORMAT parquet, COMPRESSION snappy);"]


def odbc2parquet_cmd(work):
    cmd = ["odbc2parquet", "--quiet", "query", "--column-length-limit", "65536"]
    if STEELMAN:
        cmd += ["--batch-size-memory", "1Gib"]  # bigger in-transit batch — its best fetch config
    cmd += [
        "--connection-string",
        "Driver={postgre_unicode};Server=localhost;Port=5432;Database=rivet;Uid=rivet;Pwd=rivet;",
        f"{work}/o2p.parquet", f'SELECT * FROM public."{TABLE}"',
    ]
    return cmd


def ingestr_cmd(work):
    cmd = [
        "ingestr", "ingest", "--source-uri", PG_URL, "--source-table", f"public.{TABLE}",
        "--dest-uri", f"parquet:///{work}/ing.parquet", "--dest-table", TABLE, "--yes",
    ]
    if STEELMAN:
        cmd += ["--page-size", "100000"]  # bigger fetch page (client buffer) — best throughput
    return cmd


TOOLS = [
    ("rivet (chunked)", "rivet", rivet_cmd),
    ("ingestr", "ingestr", ingestr_cmd),
    ("sling", "sling", sling_cmd),
    ("duckdb", "duckdb", duckdb_cmd),
    ("odbc2parquet", "odbc2parquet", odbc2parquet_cmd),
]


def harm_run(cmd):
    tb0, ba0 = counters()
    s = Sampler()
    s.start()
    t0 = time.monotonic()
    rc = subprocess.run(cmd, capture_output=True, text=True).returncode
    wall = time.monotonic() - t0
    s.stop = True
    s.join()
    tb1, ba1 = counters()
    return {
        "wall": wall,
        "rc": rc,
        "peak_backends": s.peak_backends,
        "longest_query_s": s.longest_query_s,
        "temp_mb": (tb1 - tb0) / 1e6,
        "blocks_accessed": ba1 - ba0,
    }


def main():
    rows = pg(f"SELECT count(*) FROM public.\"{TABLE}\"")
    print(f"# PostgreSQL harm matrix — {TABLE} ({int(rows):,} rows)\n")
    # Warm the buffer cache once so no tool is unfairly cold-first (a seq scan over
    # the heap); blocks-accessed is cache-independent anyway, this just removes any
    # cold-start bias from the wall + longest-query timings.
    pg(f'SELECT count(*) FROM public."{TABLE}"')
    results = []
    with tempfile.TemporaryDirectory() as work:
        for label, bin_, mk in TOOLS:
            if bin_ != "rivet" and not shutil.which(bin_):
                print(f"NOTE: '{bin_}' not on PATH — skipped")
                continue
            for sub in os.listdir(work):
                p = os.path.join(work, sub)
                shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.remove(p)
            r = harm_run(mk(work))
            results.append((label, r))
            print(
                f"{label:16} wall={r['wall']:6.1f}s  backends={r['peak_backends']}  "
                f"longest_q={r['longest_query_s']:6.1f}s  temp={r['temp_mb']:7.0f}MB"
                + ("" if r["rc"] == 0 else f"  [rc={r['rc']}]")
            )

    mode = " — STEELMAN (each SELECT-* tool at its best fetch config)" if STEELMAN else ""
    print(f"\n## DB-harm matrix{mode} (lower = gentler on the source)\n")
    print("| tool | wall (s) | peak backends | longest query (s) | temp spilled (MB) |")
    print("|---|---|---|---|---|")
    for label, r in results:
        rc = "" if r["rc"] == 0 else " ⚠️failed"
        print(
            f"| {label}{rc} | {r['wall']:.1f} | {r['peak_backends']} | "
            f"**{r['longest_query_s']:.1f}** | {r['temp_mb']:.0f} |"
        )
    print(
        "\n*longest query* = how long a single snapshot was held — a `SELECT *` tool "
        "pins one for the whole run (blocks VACUUM); rivet's chunked keyset holds only "
        "a single chunk at a time."
    )


if __name__ == "__main__":
    main()
