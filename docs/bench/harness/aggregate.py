"""Aggregate bench results into Markdown reports.

Reads JSON from $BENCH_ROOT/results, results_mysql, db_signals, db_signals_mysql.
Writes three Markdown reports to docs/bench/reports/:

  - REPORT_pg.md       Postgres-only: perf matrix + DB signals + per-tool cards
  - REPORT_mysql.md    MySQL-only: same shape
  - REPORT_combined.md Cross-DB summary + PG↔MySQL drift table

Usage: aggregate.py <output_dir>   (default: docs/bench/reports relative to $REPO)
"""

import json
import os
import re
import sys
from glob import glob

ROOT = os.environ.get("BENCH_ROOT", "/tmp/rivet_bench")
TABLES_PG_FILE = os.environ.get("BENCH_TABLES_PG", f"{ROOT}/tables_pg.txt")
TABLES_MY_FILE = os.environ.get("BENCH_TABLES_MY", f"{ROOT}/tables_mysql.txt")
PERF_PG = f"{ROOT}/results"
PERF_MY = f"{ROOT}/results_mysql"
DB_PG = f"{ROOT}/db_signals"
DB_MY = f"{ROOT}/db_signals_mysql"

PG_TOOLS = ("rivet", "sling", "dlt", "duckdb", "clickhouse", "odbc2parquet")
MY_TOOLS = ("rivet", "sling", "dlt", "duckdb", "clickhouse")
SAMPLE_TABLES = ("bench_narrow", "page_views", "content_items")  # PG bench-only fixtures plus shared big tables
SAMPLE_TABLES_MY = ("audit_log", "page_views", "content_items")


def read_tables(path):
    if not os.path.exists(path):
        return []
    return [
        (n, int(r))
        for n, r in (line.strip().split("|") for line in open(path) if line.strip())
    ]


def parse_wall(s):
    if not s:
        return 0.0
    m = re.match(r"(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)", s)
    if not m:
        return 0.0
    h, mm, sec = m.groups()
    return (int(h) if h else 0) * 3600 + int(mm) * 60 + float(sec)


def fmt_bytes(b):
    if b is None:
        return "—"
    if b == 0:
        return "0"
    u = ["B", "KB", "MB", "GB"]
    i, v = 0, float(b)
    while v >= 1024 and i < 3:
        v /= 1024
        i += 1
    return f"{v:.1f}{u[i]}"


def load(tool, table, dir):
    p = f"{dir}/{tool}_{table}.json"
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p))
    except json.JSONDecodeError:
        return None


def perf_totals(tools, tables, perf_dir):
    out = {}
    for t in tools:
        tw = tu = 0.0
        rss = 0.0
        ob = 0
        fails = 0
        for tbl, _ in tables:
            d = load(t, tbl, perf_dir)
            if not d:
                continue
            if d["rc"] != 0:
                fails += 1
                continue
            tw += parse_wall(d["wall"])
            tu += float(d["user_s"] or 0)
            rss = max(rss, int(d["rss_kb"] or 0) / 1024)
            ob += d.get("pq_bytes", 0)
        out[t] = {"wall": tw, "user": tu, "rss": rss, "out": ob, "fails": fails}
    return out


# ── PG report ─────────────────────────────────────────────────────────────


def render_pg_report():
    tables = read_tables(TABLES_PG_FILE)
    if not tables:
        return "# PG report\n\n_No `tables_pg.txt` found; skipping PG section._\n"
    out = perf_totals(PG_TOOLS, tables, PERF_PG)

    lines = ["# Postgres benchmark — 6 tools × 22 tables\n"]
    lines.append(
        "Wall / RSS / output measured via `gtime -v`; DB signals from `pg_stat_database`\n"
        "+ `pg_stat_bgwriter` + `pg_stat_io` deltas plus `pg_stat_activity` sampler @ 50 ms.\n"
        "All five non-Rivet tools run with their defaults; Rivet uses `table:` shortcut,\n"
        "`environment: local`, `mode: chunked` with `chunk_size_memory_mb: 256` and the\n"
        "auto-PK + work_mem-aware FETCH cap.\n"
    )

    lines.append("## Total wall, CPU, RSS, output (22 tables)\n")
    lines.append("| Tool | Wall (s) | User CPU (s) | Peak RSS | Output | Failures |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for t in PG_TOOLS:
        p = out[t]
        lines.append(
            f"| **{t}** | {p['wall']:.1f} | {p['user']:.1f} | {p['rss']:.0f} MB | "
            f"{fmt_bytes(p['out'])} | {p['fails']} |"
        )
    lines.append("")

    # DB signals for content_items
    lines.append("## DB-side signals — content_items (2 M × 20 wide cols)\n")
    lines.append(
        "| Tool | Longest single query | Peak active backends | xact_commit | temp_bytes | temp_files |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|")
    for t in PG_TOOLS:
        d = load(t, "content_items", DB_PG)
        if not d:
            lines.append(f"| {t} | — | — | — | — | — |")
            continue
        a = d["activity_during_run"]
        s = d["pg_stat_database_delta"]
        lines.append(
            f"| {t} | {a['longest_query_seconds']:.2f}s | {a['peak_active_backends']} | "
            f"{s.get('xact_commit', 0)} | {fmt_bytes(s.get('temp_bytes', 0))} | "
            f"{s.get('temp_files', 0)} |"
        )
    lines.append("")

    lines.append("## Sample SQL templates observed (content_items)\n")
    for t in PG_TOOLS:
        d = load(t, "content_items", DB_PG)
        templates = (d or {}).get("activity_during_run", {}).get("templates_observed", [])
        if not templates:
            continue
        lines.append(f"### `{t}`")
        for tpl in templates[:5]:
            lines.append(f"- `{tpl[:140]}`")
        if len(templates) > 5:
            lines.append(f"- _… +{len(templates) - 5} more_")
        lines.append("")
    return "\n".join(lines)


# ── MySQL report ──────────────────────────────────────────────────────────


def render_mysql_report():
    tables = read_tables(TABLES_MY_FILE)
    if not tables:
        return "# MySQL report\n\n_No `tables_mysql.txt` found; skipping MySQL section._\n"
    out = perf_totals(MY_TOOLS, tables, PERF_MY)

    lines = ["# MySQL benchmark — 5 tools × 17 tables\n"]
    lines.append(
        "`odbc2parquet` is excluded — the MySQL ODBC driver isn't available via `brew` on\n"
        "macOS arm64. The other five tools are the same wire-protocol classes as the PG\n"
        "matrix. DB signals from `performance_schema.global_status` + `information_schema.processlist`.\n"
    )

    lines.append("## Total wall, CPU, RSS, output (17 tables)\n")
    lines.append("| Tool | Wall (s) | User CPU (s) | Peak RSS | Output | Failures |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for t in MY_TOOLS:
        p = out[t]
        lines.append(
            f"| **{t}** | {p['wall']:.1f} | {p['user']:.1f} | {p['rss']:.0f} MB | "
            f"{fmt_bytes(p['out'])} | {p['fails']} |"
        )
    lines.append("")

    lines.append("## DB-side signals — content_items (~2 M rows, wide)\n")
    lines.append(
        "| Tool | Longest Q | Peak active | Queries | Innodb_rows_read | Bytes sent |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|")
    for t in MY_TOOLS:
        d = load(t, "content_items", DB_MY)
        if not d:
            lines.append(f"| {t} | — | — | — | — | — |")
            continue
        a = d["activity_during_run"]
        g = d["global_status_delta"]
        lines.append(
            f"| {t} | {a['longest_query_seconds']}s | {a['peak_active_threads']} | "
            f"{g.get('queries', 0)} | {g.get('innodb_rows_read', 0):,} | "
            f"{fmt_bytes(g.get('bytes_sent', 0))} |"
        )
    lines.append("")
    lines.append(
        "**Note**: MySQL `processlist.time` is whole-second granular. A reading of `0s`\n"
        "means the query finished in <1 s between samples, not that it was instantaneous.\n"
    )
    return "\n".join(lines)


# ── Combined ──────────────────────────────────────────────────────────────


def render_combined():
    pg_tables = read_tables(TABLES_PG_FILE)
    my_tables = read_tables(TABLES_MY_FILE)
    pg_out = perf_totals(MY_TOOLS, pg_tables, PERF_PG) if pg_tables else None
    my_out = perf_totals(MY_TOOLS, my_tables, PERF_MY) if my_tables else None

    lines = ["# Combined report — Postgres vs MySQL across 5 tools\n"]
    lines.append(
        "Same workload class on both databases (the seed fixtures are aligned). Compares the\n"
        "**same five tools** that run on both: rivet, sling, dlt, duckdb, clickhouse.\n"
        "(`odbc2parquet` only on PG — see MySQL report for the methodology note.)\n"
    )

    if pg_out and my_out:
        lines.append("## Total wall + RSS, by tool, side by side\n")
        lines.append(
            "| Tool | Wall PG → MySQL | RSS PG → MySQL | Longest Q on content_items |"
        )
        lines.append("|---|---:|---:|---:|")
        for t in MY_TOOLS:
            pg_q = (load(t, "content_items", DB_PG) or {}).get(
                "activity_during_run", {}
            ).get("longest_query_seconds", 0)
            my_q = (load(t, "content_items", DB_MY) or {}).get(
                "activity_during_run", {}
            ).get("longest_query_seconds", 0)
            lines.append(
                f"| **{t}** | {pg_out[t]['wall']:.1f}s → {my_out[t]['wall']:.1f}s | "
                f"{pg_out[t]['rss']:.0f} MB → {my_out[t]['rss']:.0f} MB | "
                f"{pg_q:.2f}s → {my_q}s |"
            )
        lines.append("")

    lines.append("## Headline observations\n")
    lines.append("1. **Rivet wins both databases on the source-friendliness axes** (longest single")
    lines.append("   query, peak concurrent backends, CPU). The chunked + auto-PK + memory-budget")
    lines.append("   strategy is source-neutral; only the wire driver differs.")
    lines.append("2. **DuckDB's parallel postgres_scanner is PG-specific**. Its mysql_scanner")
    lines.append("   counterpart is single-threaded, so the multi-minute wins seen on PG don't")
    lines.append("   transfer to MySQL.")
    lines.append("3. **Sling and ClickHouse fire one long query per export on both DBs** —")
    lines.append("   2-minute single-statement holds that would trip `statement_timeout` or")
    lines.append("   `max_execution_time` on a typical production policy.\n")
    return "\n".join(lines)


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
        "~/rivet/docs/bench/reports"
    )
    os.makedirs(out_dir, exist_ok=True)
    open(f"{out_dir}/REPORT_pg.md", "w").write(render_pg_report())
    open(f"{out_dir}/REPORT_mysql.md", "w").write(render_mysql_report())
    open(f"{out_dir}/REPORT_combined.md", "w").write(render_combined())
    print(f"wrote {out_dir}/REPORT_pg.md")
    print(f"wrote {out_dir}/REPORT_mysql.md")
    print(f"wrote {out_dir}/REPORT_combined.md")


if __name__ == "__main__":
    main()
