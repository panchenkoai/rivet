#!/usr/bin/env python3
"""Type-fidelity report aggregator (sibling of perf bench `aggregate.py`).

Reads every Parquet under ``$BENCH_ROOT/<source>/<tool>/<table>/data.parquet``
that ``type_bench.sh`` produced, pulls per-column ``(physical, logical, value
sample)`` triples through pyarrow, and prints a Markdown matrix that compares
how each tool exported every type on the canonical
``rivet_type_matrix`` / ``rivet_type_matrix_full`` tables.

Tools that failed to produce a Parquet (refused conversion, crashed, were
skipped) show ``FAIL`` in their cell with the last line of stderr surfaced
in a footnote — the missing-Parquet failure mode *is* the comparison data.

Designed to run inside the ``rivet-duckdb`` container which already has
``pyarrow`` installed::

    docker exec rivet-duckdb python /work/.../type_diff.py /work/type_bench

When invoked locally outside the container, point at the host path and
have pyarrow on PATH.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq


# ── Reference: what each (source, table, column) *should* look like ─────────
# This is the "ground truth" Rivet aims at — taken from
# `tests/type_roundtrip/fixtures/expected_contracts.yaml` plus the
# additional contract pinned by `tests/type_roundtrip/parquet_schema.rs`.
#
# Format per column:  source_native, ideal_physical, ideal_logical
# The classifier compares each tool's actual output against `ideal_*` and
# falls back to the helpers below to grade common downgrades.

REFERENCE: dict[tuple[str, str], list[tuple[str, str, str, str | None]]] = {
    ("pg", "rivet_type_matrix"): [
        # column,        source_native, ideal_physical,           ideal_logical
        ("id",           "bigint",      "INT64",                  None),
        ("label",        "text",        "BYTE_ARRAY",             "String"),
        ("amount",       "numeric(18,2)","INT64",                 "Decimal(18,2)"),
        ("fee",          "numeric(18,6)","INT64",                 "Decimal(18,6)"),
        ("created_at",   "timestamp",   "INT64",                  "Timestamp(MICROS,naive)"),
        ("created_at_tz","timestamptz", "INT64",                  "Timestamp(MICROS,UTC)"),
        ("raw_bytes",    "bytea",       "BYTE_ARRAY",             None),
        ("uid",          "uuid",        "BYTE_ARRAY",             "String"),
        ("attrs",        "jsonb",       "BYTE_ARRAY",             "String"),
    ],
    ("pg", "rivet_type_matrix_full"): [
        ("id",           "bigint",      "INT64",                  None),
        ("flag",         "boolean",     "BOOLEAN",                None),
        ("int2_col",     "smallint",    "INT32",                  "Integer(bit_width=16,signed=True)"),
        ("int4_col",     "integer",     "INT32",                  None),
        ("float4_col",   "real",        "FLOAT",                  None),
        ("date_col",     "date",        "INT32",                  "Date"),
        ("time_col",     "time",        "INT64",                  "Time(MICROS,naive)"),
        ("interval_col", "interval",    "BYTE_ARRAY",             "String"),
        ("enum_col",     "enum",        "BYTE_ARRAY",             "String"),
        ("tags",         "text[]",      "LIST<BYTE_ARRAY>",       "List<String>"),
        ("nums",         "integer[]",   "LIST<INT32>",            "List"),
    ],
    ("mysql", "rivet_type_matrix"): [
        ("id",           "bigint",      "INT64",                  None),
        ("label",        "varchar",     "BYTE_ARRAY",             "String"),
        ("amount",       "decimal(18,2)","INT64",                 "Decimal(18,2)"),
        ("fee",          "decimal(18,6)","INT64",                 "Decimal(18,6)"),
        ("created_at_dt","datetime",    "INT64",                  "Timestamp(MICROS,naive)"),
        ("created_at_ts","timestamp",   "INT64",                  "Timestamp(MICROS,UTC)"),
        ("raw_bytes",    "binary(4)",   "BYTE_ARRAY",             None),
        ("uid",          "varchar(36)", "BYTE_ARRAY",             "String"),
        ("extras",       "json",        "BYTE_ARRAY",             "String"),
    ],
    ("mysql", "rivet_type_matrix_full"): [
        ("id",           "bigint",      "INT64",                  None),
        ("flag",         "tinyint(1)",  "BOOLEAN",                None),
        ("bit1_col",     "bit(1)",      "BOOLEAN",                None),
        ("bit8_col",     "bit(8)",      "INT64",                  None),
        ("tiny_col",     "tinyint",     "INT32",                  "Integer(bit_width=16,signed=True)"),
        ("date_col",     "date",        "INT32",                  "Date"),
        ("time_col",     "time(6)",     "INT64",                  "Time(MICROS,naive)"),
        ("year_col",     "year",        "INT32",                  "Integer(bit_width=16,signed=True)"),
        ("enum_col",     "enum",        "BYTE_ARRAY",             "String"),
        ("varbinary_col","varbinary(4)","BYTE_ARRAY",             None),
        ("blob_col",     "blob",        "BYTE_ARRAY",             None),
    ],
}

TOOLS = ["rivet", "sling", "duckdb", "clickhouse", "odbc2parquet"]


# ── Pyarrow-side helpers ────────────────────────────────────────────────────


def _logical_str(logical) -> str | None:
    """Render a parquet LogicalType into the compact form used by REFERENCE.

    pyarrow returns a logical-type sentinel object whose `str()` is the
    literal `"None"` for columns without a logical type — collapse both
    that and an actual Python `None` to None here so the classifier sees a
    single absence value.
    """
    if logical is None:
        return None
    s = str(logical)
    if s in {"None", "NONE", "NoneType"}:
        return None
    # pyarrow prints e.g. "String", "Decimal(precision=18, scale=2)",
    # "Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, ...)",
    # "Time(...)", "Date", "Int(bitWidth=16, isSigned=true)".
    # Normalise to the keys used in REFERENCE so they're directly comparable.
    if s.startswith("Decimal("):
        # "Decimal(precision=18, scale=2)"
        import re
        m = re.search(r"precision=(\d+), scale=(\d+)", s)
        if m:
            return f"Decimal({m.group(1)},{m.group(2)})"
    if s.startswith("Timestamp("):
        adj = "UTC" if "isAdjustedToUTC=true" in s else "naive"
        unit = "MICROS" if "timeUnit=microseconds" in s \
               else "MILLIS" if "timeUnit=milliseconds" in s \
               else "NANOS" if "timeUnit=nanoseconds" in s else "?"
        return f"Timestamp({unit},{adj})"
    if s.startswith("Time("):
        adj = "UTC" if "isAdjustedToUTC=true" in s else "naive"
        unit = "MICROS" if "timeUnit=microseconds" in s \
               else "MILLIS" if "timeUnit=milliseconds" in s \
               else "NANOS" if "timeUnit=nanoseconds" in s else "?"
        return f"Time({unit},{adj})"
    if s.startswith("Int("):
        import re
        m = re.search(r"bitWidth=(\d+), isSigned=(true|false)", s)
        if m:
            bw, signed = m.group(1), (m.group(2) == "true")
            return f"Integer(bit_width={bw},signed={signed})"
    return s


def _physical_str(t) -> str:
    """Render parquet physical type, recursing into LIST inner type."""
    s = str(t)
    # Group with logical=List has children → recurse so reader can see e.g.
    # "LIST<BYTE_ARRAY>" or "LIST<INT32>".
    return s


def _column_summary(pqfile: pq.ParquetFile, name: str) -> tuple[str, str | None]:
    """Return (physical, logical) for a top-level column, handling lists."""
    schema = pqfile.schema_arrow  # arrow side carries the friendliest names
    field = None
    for f in schema:
        if f.name == name:
            field = f
            break
    if field is None:
        return ("MISSING", None)

    # For lists we want the inner element's parquet type, which lives in the
    # *physical* schema (pqfile.schema). Walk every leaf.
    pq_schema = pqfile.schema
    # pyarrow ParquetSchema does not expose num_columns directly; iterate
    # via `column(i)` until IndexError. There are len(pq_schema) leaves.
    n_leaves = len(pq_schema)
    for i in range(n_leaves):
        col = pq_schema.column(i)
        path = col.path
        if path == name or path.startswith(name + "."):
            phys = col.physical_type
            logical = _logical_str(col.logical_type)
            # If this is a list inner element, wrap it.
            if path != name:
                return (f"LIST<{phys}>", logical or "List")
            return (phys, logical)
    return ("MISSING", None)


def _stderr_tail(stderr_path: Path) -> str:
    """Last non-empty line of stderr.log, trimmed."""
    if not stderr_path.exists():
        return "(no stderr)"
    lines = [
        ln.strip() for ln in stderr_path.read_text(errors="replace").splitlines() if ln.strip()
    ]
    return lines[-1] if lines else "(empty stderr)"


# ── Classification ──────────────────────────────────────────────────────────

# Glyphs for the matrix:
#   ✓  exact     — physical+logical match the reference
#   ~  compat    — values preserved but representation differs
#   ✗  lossy     — silent type degradation (e.g. decimal→double)
#   ✗! fail      — no Parquet produced
#   .  missing   — tool was not configured to handle this case

def _classify(actual_phys: str, actual_log: str | None,
              ideal_phys: str, ideal_log: str | None) -> str:
    if actual_phys == "MISSING":
        return "MISSING"
    if actual_phys == ideal_phys and actual_log == ideal_log:
        return "EXACT"

    # ── upgrades / better metadata ──────────────────────────────────────────
    # Tool emits Parquet `LogicalType::Uuid` for a UUID column (Rivet
    # currently writes `String` + `rivet.logical_type=uuid` field metadata).
    # That's a *better* representation, not a regression — call it EXACT.
    if ideal_log == "String" and actual_log == "UUID":
        return "EXACT_BETTER"
    # Same for `LogicalType::Json` on a JSON column.
    if ideal_log == "String" and actual_log == "JSON":
        return "EXACT_BETTER"
    # Same physical type, tool added an `Integer(bit_width=...)` annotation
    # where Rivet didn't — extra metadata, no loss of fidelity.
    if (actual_phys == ideal_phys
            and ideal_log is None
            and actual_log and actual_log.startswith("Integer(")):
        return "EXACT"

    # ── list structure flattened to string ──────────────────────────────────
    if ideal_phys.startswith("LIST<") and not actual_phys.startswith("LIST<"):
        return "LOSSY_LIST"

    # ── decimal degradations ────────────────────────────────────────────────
    if ideal_log and ideal_log.startswith("Decimal") and actual_phys in {"DOUBLE", "FLOAT"}:
        return "LOSSY"
    if ideal_log and ideal_log.startswith("Decimal") and actual_log == "String":
        return "COMPAT"
    # Sling autodetects decimal precision/scale from data, often coarsening
    # to Decimal(24,6). Values fit, but precision contract is lost.
    if (ideal_log and ideal_log.startswith("Decimal")
            and actual_log and actual_log.startswith("Decimal")
            and actual_log != ideal_log):
        return "COMPAT_DEC"

    # ── tz dropped ──────────────────────────────────────────────────────────
    if (ideal_log and ideal_log.startswith("Timestamp(") and "UTC" in ideal_log
            and actual_log and "naive" in actual_log):
        return "LOSSY_TZ"

    # ── narrower int (i32 → i16/i8) for an i64 column ───────────────────────
    if ideal_phys == "INT64" and actual_phys in {"INT32", "INT16"}:
        return "LOSSY"

    # ── binary stored as string (raw bytes survive but encoding suspect) ────
    if ideal_phys == "BYTE_ARRAY" and ideal_log is None and actual_log == "String":
        return "COMPAT"

    # ── different physical but logical aligns ───────────────────────────────
    if actual_log == ideal_log and actual_log is not None:
        return "COMPAT"

    return "COMPAT"


GLYPH = {
    "EXACT":        "✓",
    "EXACT_BETTER": "✓+",
    "COMPAT":       "~",
    "COMPAT_DEC":   "~dec",
    "LOSSY":        "✗",
    "LOSSY_TZ":     "✗tz",
    "LOSSY_LIST":   "✗list",
    "MISSING":      ".",
    "FAIL":         "✗!",
}


# ── Main report builder ─────────────────────────────────────────────────────


def run(bench_root: Path) -> None:
    print(f"# Type-fidelity benchmark\n")
    print(f"Sibling of [`REPORT_pg.md`](REPORT_pg.md) — same six tools, same")
    print(f"`rivet_type_matrix` / `rivet_type_matrix_full` seed tables — but")
    print(f"the question is _what does the Parquet schema look like after each")
    print(f"tool exports it?_, not how fast / how much RSS. The headline data")
    print(f"point per cell is the Parquet ``physical+logical`` type pair as")
    print(f"reported by pyarrow; the glyph is a classification against the")
    print(f"Rivet reference. See the legend at the bottom.\n")
    print(f"Reproduce: `docs/bench/harness/type_bench.sh all` then")
    print(f"`docker exec -i rivet-duckdb python - /work/type_bench <")
    print(f"docs/bench/harness/type_diff.py > docs/bench/reports/REPORT_types.md`.\n")

    # ── Pre-scan: build a 2-D map source/table/tool → {col → (phys, log) | "FAIL"}
    grids: dict[tuple[str, str], dict[str, dict[str, tuple[str, str | None]] | str]] = {}
    errors: dict[tuple[str, str, str], str] = {}
    for (source, table), columns in REFERENCE.items():
        out_root = bench_root / source
        if not out_root.exists():
            continue
        per_tool: dict[str, dict[str, tuple[str, str | None]] | str] = {}
        for tool in TOOLS:
            tdir = out_root / tool / table
            pq_files = sorted(tdir.glob("*.parquet")) if tdir.exists() else []
            stderr = tdir / "stderr.log"
            if not pq_files:
                per_tool[tool] = "FAIL"
                errors[(source, table, tool)] = _stderr_tail(stderr)
                continue
            try:
                pf = pq.ParquetFile(pq_files[0])
            except Exception as e:
                per_tool[tool] = "FAIL"
                errors[(source, table, tool)] = f"parquet open failed: {e}"
                continue
            cols: dict[str, tuple[str, str | None]] = {}
            for c, *_ in columns:
                cols[c] = _column_summary(pf, c)
            per_tool[tool] = cols
        grids[(source, table)] = per_tool

    # ── Headline summary: aggregate cell counts per (source, tool) ──────────
    print("## Headline\n")
    print("Cell counts per source — `✓` includes `✓+` upgrades, `✗*` is any")
    print("loss class, `✗!` is whole-table failure.\n")
    for src in ["pg", "mysql"]:
        print(f"### {src.upper()}\n")
        print("| tool | ✓ exact | ✓+ richer | ~ compat | ✗ lossy | ✗! failed |")
        print("|------|--------:|----------:|---------:|--------:|----------:|")
        for tool in TOOLS:
            tot = {"EXACT": 0, "EXACT_BETTER": 0, "COMPAT": 0, "LOSSY": 0, "FAIL": 0}
            for (s, t), per_tool in grids.items():
                if s != src:
                    continue
                rec = per_tool[tool]
                cols = REFERENCE[(s, t)]
                if rec == "FAIL":
                    tot["FAIL"] += len(cols)
                    continue
                for c, _src, ip, il in cols:
                    phys, log = rec[c]  # type: ignore[index]
                    if phys == "MISSING":
                        continue
                    cls = _classify(phys, log, ip, il)
                    if cls == "EXACT":
                        tot["EXACT"] += 1
                    elif cls == "EXACT_BETTER":
                        tot["EXACT_BETTER"] += 1
                    elif cls.startswith("LOSSY"):
                        tot["LOSSY"] += 1
                    else:
                        tot["COMPAT"] += 1
            print(f"| {tool} | {tot['EXACT']} | {tot['EXACT_BETTER']} | "
                  f"{tot['COMPAT']} | {tot['LOSSY']} | {tot['FAIL']} |")
        print()

    print("---\n")
    print("## Per-table detail\n")

    for (source, table), columns in REFERENCE.items():
        out_root = bench_root / source
        if not out_root.exists():
            continue
        per_tool = grids[(source, table)]
        out_root = bench_root / source
        if not out_root.exists():
            continue
        print(f"### {source.upper()} · `{table}`\n")
        per_tool_err = {t: errors.get((source, table, t), "") for t in TOOLS}

        # Header
        header = "| column | source type | " + " | ".join(TOOLS) + " |"
        print(header)
        print("|" + "---|" * (2 + len(TOOLS)))

        for col, src_native, ideal_phys, ideal_log in columns:
            row = [f"`{col}`", f"`{src_native}`"]
            for tool in TOOLS:
                rec = per_tool[tool]
                if rec == "FAIL":
                    row.append("**✗!**")
                    continue
                phys, log = rec[col]  # type: ignore[index]
                if phys == "MISSING":
                    row.append("`.`")
                    continue
                cls = _classify(phys, log, ideal_phys, ideal_log)
                glyph = GLYPH[cls]
                repr_ = phys if log is None else f"{phys}+{log}"
                row.append(f"{glyph} `{repr_}`")
            print("| " + " | ".join(row) + " |")

        # Footnotes for failures and notes.
        any_fail = any(per_tool[t] == "FAIL" for t in TOOLS)
        if any_fail:
            print()
            for tool in TOOLS:
                if per_tool[tool] == "FAIL":
                    err = per_tool_err.get(tool, "")
                    if "no Rivet mapping" in err:
                        note = "Rivet refused — column override required (no silent degradation)"
                    elif "not configured" in err.lower() or "skip" in err.lower():
                        note = "tool not configured for this source (no comparison data point)"
                    else:
                        note = err[:200]
                    print(f"- **{tool}**: ✗! — {note}")
        print()

    # Summary glyph legend
    print("---")
    print("\n### Legend\n")
    print("| Glyph | Meaning |")
    print("|-------|---------|")
    print("| `✓` | Exact match against the Rivet reference (same physical + logical type) |")
    print("| `✓+` | Tool emits **richer** Parquet logical type than Rivet (e.g. native `UUID` / `JSON` logical type vs `String`) |")
    print("| `~` | Different physical/logical but values preserved |")
    print("| `~dec` | Decimal precision/scale coarsened (e.g. `Decimal(18,2)` → `Decimal(24,6)`) — values fit but the column contract is widened |")
    print("| `✗` | Silent lossy degradation (e.g. `Decimal(p,s)` → `DOUBLE` loses precision past 2⁵³) |")
    print("| `✗tz` | Timezone information dropped from a tz-aware timestamp |")
    print("| `✗list` | LIST structure flattened to a string blob — array semantics lost |")
    print("| `✗!` | Tool failed to produce a Parquet at all — the failure mode itself is the comparison |")
    print("| `.` | Tool not configured for this source/table (no data point) |")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: type_diff.py <bench_root>", file=sys.stderr)
        sys.exit(2)
    run(Path(sys.argv[1]))
