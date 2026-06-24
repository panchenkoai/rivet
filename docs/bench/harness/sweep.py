#!/usr/bin/env python3
"""Row-size sweep for the rivet bench — driven by ../scenarios.yaml.

For each scale it slices the wide `content_items` fixture to that row count (CTAS
LIMIT), then runs each tool `warmup` + `runs` times under `/usr/bin/time -l`,
recording wall time and peak RSS, and reports the MEDIAN per (scale, tool) as a
markdown table.

The point of the sweep is the SHAPE of the RSS curve: rivet sizes its work to a
byte budget, so peak RSS stays ~flat as the row count grows; a tool that batches a
fixed ROW count (e.g. ingestr's 100k-row Arrow batches) climbs with row width ×
batch. A single fixed-size run can't show that — the sweep can.

Usage:
    python3 docs/bench/harness/sweep.py
Env:
    RIVET_BIN   path to the rivet binary (default: a downloaded release under /tmp)
    PG_CONTAINER  docker container name for the bench Postgres (default rivet-postgres-1)
"""

import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCENARIOS = os.path.join(HERE, "..", "scenarios.yaml")

PG_CONTAINER = os.environ.get("PG_CONTAINER", "rivet-postgres-1")
PG_URL = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
RIVET = os.environ.get(
    "RIVET_BIN", "/tmp/rivet_release/rivet-v0.14.0-aarch64-apple-darwin/rivet"
)


def load_scenarios():
    """Minimal reader for the flat scenarios.yaml (no PyYAML dependency on the host)."""
    cfg = {}
    with open(SCENARIOS) as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, val = (s.strip() for s in line.split(":", 1))
            if val.startswith("[") and val.endswith("]"):
                items = [x.strip() for x in val[1:-1].split(",") if x.strip()]
                cfg[key] = [int(x) if x.isdigit() else x for x in items]
            elif val.isdigit():
                cfg[key] = int(val)
            else:
                cfg[key] = val
    return cfg


def psql(sql):
    subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "rivet", "-d", "rivet", "-c", sql],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def seeded_rows():
    out = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", "rivet", "-d", "rivet",
         "-tAc", "SELECT count(*) FROM content_items"],
        capture_output=True, text=True,
    )
    return int(out.stdout.strip() or 0)


def make_scale(scale):
    t = f"content_items_{scale}"
    psql(f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} AS SELECT * FROM content_items LIMIT {scale};")
    return t


def timed(cmd):
    """Run `cmd` under /usr/bin/time -l; return (wall_seconds, rss_mb, returncode)."""
    p = subprocess.run(["/usr/bin/time", "-l"] + cmd, capture_output=True, text=True)
    wall = float(re.search(r"([\d.]+)\s+real", p.stderr).group(1))
    rss = int(re.search(r"(\d+)\s+maximum resident set size", p.stderr).group(1)) / 1e6
    return wall, rss, p.returncode


def run_rivet(table, work):
    cfg = os.path.join(work, "rivet.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{ type: postgres, url: "{PG_URL}" }}\n'
            f"exports:\n"
            f"  - name: {table}\n"
            f'    query: "SELECT * FROM public.{table}"\n'
            f"    mode: full\n"
            f"    format: parquet\n"
            f"    compression: snappy\n"
            f'    destination: {{ type: local, path: "{work}/rivet_out" }}\n'
        )
    shutil.rmtree(os.path.join(work, "rivet_out"), ignore_errors=True)
    return timed([RIVET, "run", "--config", cfg])


def run_ingestr(table, work):
    out = os.path.join(work, "ing.parquet")
    if os.path.exists(out):
        os.remove(out)
    return timed([
        "ingestr", "ingest",
        "--source-uri", PG_URL,
        "--source-table", f"public.{table}",
        "--dest-uri", f"parquet:///{out}",
        "--dest-table", table, "--yes",
    ])


RUNNERS = {"rivet": run_rivet, "ingestr": run_ingestr}


def median_run(tool, table, runs, warmup, work):
    fn = RUNNERS[tool]
    for _ in range(warmup):
        fn(table, work)  # discarded
    walls, rsss = [], []
    for _ in range(runs):
        w, r, rc = fn(table, work)
        if rc != 0:
            print(f"  WARN: {tool} exited {rc} at {table}", file=sys.stderr)
        walls.append(w)
        rsss.append(r)
    return statistics.median(walls), statistics.median(rsss)


def main():
    cfg = load_scenarios()
    scales, runs, warmup = cfg["scales"], cfg["runs"], cfg["warmup"]
    tools = [
        t for t in cfg["tools"]
        if t == "rivet" or shutil.which(t) or print(f"NOTE: '{t}' not on PATH — skipped")
    ]
    seeded = seeded_rows()

    results = []
    with tempfile.TemporaryDirectory() as work:
        for scale in scales:
            if scale > seeded:
                print(f"NOTE: scale {scale:,} > seeded {seeded:,} — skipped (re-seed for bigger)")
                continue
            table = make_scale(scale)
            for tool in tools:
                w, r = median_run(tool, table, runs, warmup, work)
                results.append((scale, tool, w, r))
                print(f"scale={scale:>9,}  {tool:8}  wall={w:6.1f}s  rss={r:7.0f}MB")
            psql(f"DROP TABLE IF EXISTS {table};")

    print(f"\n## Row-size sweep — median of {runs} runs (+{warmup} warmup)\n")
    print("| scale | tool | wall (s) | peak RSS (MB) |")
    print("|---|---|---|---|")
    for scale, tool, w, r in results:
        print(f"| {scale:,} | {tool} | {w:.1f} | {r:.0f} |")
    # RSS ratio per scale (the flatness story), when >1 tool ran
    by_scale = {}
    for scale, tool, _w, r in results:
        by_scale.setdefault(scale, {})[tool] = r
    if any(len(v) > 1 for v in by_scale.values()):
        print("\nPeak-RSS ratio (other / rivet) — the structural gap, ~constant across row count")
        print("(it widens with row WIDTH, not count — see scenarios.yaml):")
        for scale, v in by_scale.items():
            if "rivet" in v:
                ratios = ", ".join(f"{t}={v[t]/v['rivet']:.0f}x" for t in v if t != "rivet")
                print(f"  {scale:,}: {ratios}")


if __name__ == "__main__":
    main()
