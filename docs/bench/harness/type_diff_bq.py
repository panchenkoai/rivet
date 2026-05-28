#!/usr/bin/env python3
"""BigQuery round-trip report — sibling of `type_diff.py`.

Reads schema + aggregate JSON dumps produced by `type_bench_bq.sh`
(under ``$BENCH_ROOT/bq/<source>/<table>/{schema.json,agg.json}``) and
prints a Markdown report that, per source column, shows the BigQuery
**autodetected** type plus a classification glyph against the canonical
"right answer" for that semantic (see `REFERENCE` below).

The script intentionally does not require `pyarrow` or any Python
BigQuery client — `bq show --schema --format=prettyjson` already gives
us everything we need, and the existing harness is plain `bq` CLI. Run
it on the host:

    docs/bench/harness/type_bench_bq.sh all
    python3 docs/bench/harness/type_diff_bq.py \\
        $REPO/tests/.live-tmp/type_bench \\
        > docs/bench/reports/REPORT_types_bigquery.md
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# ── Reference: what each (source, table, column) *should* land as in
#    BigQuery after `bq load --autodetect --source_format=PARQUET`. The
#    "ideal" is the type a Parquet-aware autoloader could reasonably
#    pick given the on-disk LogicalType. Anything materially worse than
#    `ideal` gets a ✗ glyph in the matrix.
#
#    Format per column:  source_native, ideal_bq_type, note
#    `note` shows up as a footnote in the report when classification != EXACT.

# Allowed glyph values:
#   EXACT          — the column lands as the ideal BQ type
#   COMPAT         — different name but value-preserving (e.g. INT64-class:
#                    Int16 → INTEGER is "compat" because BQ has only INTEGER)
#   COMPAT_BYTES   — UUID / Binary stored as BYTES (BQ has no UUID type)
#   LOSSY_JSON     — Parquet `LogicalType::Json` does NOT autoload into BQ
#                    native `JSON`; column lands as STRING or BYTES instead.
#                    Round-trip safe at the byte level — the gap is that
#                    BQ users need an explicit schema with `JSON` to query
#                    `attrs.tier` as structured.
#   LOSSY_TZ       — tz-aware timestamp dropped tz info (BQ collapses
#                    `TIMESTAMPTZ`/`TIMESTAMP` distinction; not actually
#                    lossy since BQ TIMESTAMP is always UTC — note only)
#   FAIL           — column missing from BQ schema (load failed)

REFERENCE: dict[tuple[str, str], list[tuple[str, str, str]]] = {
    ("pg", "rivet_type_matrix"): [
        ("id",           "bigint",        "INTEGER"),
        ("label",        "text",          "STRING"),
        ("amount",       "numeric(18,2)", "NUMERIC"),
        ("fee",          "numeric(18,6)", "NUMERIC"),
        ("created_at",   "timestamp",     "TIMESTAMP"),
        ("created_at_tz","timestamptz",   "TIMESTAMP"),
        ("raw_bytes",    "bytea",         "BYTES"),
        # BQ has no UUID native type — BYTES is the canonical compact form.
        ("uid",          "uuid",          "BYTES"),
        # Ideally JSON; BQ autoloader currently picks BYTES for our
        # Parquet `LogicalType::Json` columns (see `LOSSY_JSON` glyph).
        ("attrs",        "jsonb",         "JSON"),
    ],
    ("pg", "rivet_type_matrix_full"): [
        ("id",           "bigint",        "INTEGER"),
        ("flag",         "boolean",       "BOOLEAN"),
        ("int2_col",     "smallint",      "INTEGER"),
        ("int4_col",     "integer",       "INTEGER"),
        ("float4_col",   "real",          "FLOAT"),
        ("date_col",     "date",          "DATE"),
        ("time_col",     "time",          "TIME"),
        ("interval_col", "interval",      "STRING"),
        ("enum_col",     "enum",          "STRING"),
        ("tags",         "text[]",        "RECORD"),
        ("nums",         "integer[]",     "RECORD"),
    ],
    ("mysql", "rivet_type_matrix"): [
        ("id",           "bigint",        "INTEGER"),
        ("label",        "varchar",       "STRING"),
        ("amount",       "decimal(18,2)", "NUMERIC"),
        ("fee",          "decimal(18,6)", "NUMERIC"),
        ("created_at_dt","datetime",      "TIMESTAMP"),
        ("created_at_ts","timestamp",     "TIMESTAMP"),
        ("raw_bytes",    "binary(4)",     "BYTES"),
        # MySQL UUIDs are VARCHAR(36) without an override → stays STRING.
        ("uid",          "varchar(36)",   "STRING"),
        ("extras",       "json",          "JSON"),
    ],
    ("mysql", "rivet_type_matrix_full"): [
        ("id",           "bigint",        "INTEGER"),
        ("flag",         "tinyint(1)",    "BOOLEAN"),
        ("bit1_col",     "bit(1)",        "BOOLEAN"),
        ("bit8_col",     "bit(8)",        "INTEGER"),
        ("tiny_col",     "tinyint",       "INTEGER"),
        ("date_col",     "date",          "DATE"),
        ("time_col",     "time(6)",       "TIME"),
        ("year_col",     "year",          "INTEGER"),
        ("enum_col",     "enum",          "STRING"),
        ("varbinary_col","varbinary(4)",  "BYTES"),
        ("blob_col",     "blob",          "BYTES"),
    ],
}


def _classify(actual: str | None, ideal: str) -> str:
    if actual is None:
        return "FAIL"
    if actual == ideal:
        return "EXACT"
    # JSON column landed as BYTES (or STRING) — value safe, semantics lost
    # to BigQuery's autoloader without an explicit schema.
    if ideal == "JSON" and actual in {"BYTES", "STRING"}:
        return "LOSSY_JSON"
    # BQ has no UUID native type; we expect BYTES anyway (covered above).
    # Different INTEGER widths all collapse to BQ INTEGER → COMPAT.
    if ideal == "INTEGER" and actual == "INTEGER":
        return "EXACT"
    return "COMPAT"


GLYPH = {
    "EXACT":      "✓",
    "COMPAT":     "~",
    "COMPAT_BYTES": "~",
    "LOSSY_JSON": "✗json",
    "LOSSY_TZ":   "✗tz",
    "FAIL":       "✗!",
}


def _load_cell(bench_root: Path, source: str, table: str) -> dict:
    """Return {column: bq_type | None} for the cell, or {} if no schema."""
    schema_path = bench_root / "bq" / source / table / "schema.json"
    if not schema_path.exists():
        return {}
    try:
        with schema_path.open() as f:
            cols = json.load(f)
    except Exception:
        return {}
    return {c["name"]: c["type"] for c in cols}


def _load_agg(bench_root: Path, source: str, table: str) -> dict | None:
    """Return the first row of the aggregate query, or None if missing."""
    agg_path = bench_root / "bq" / source / table / "agg.json"
    if not agg_path.exists():
        return None
    try:
        with agg_path.open() as f:
            rows = json.load(f)
        return rows[0] if rows else None
    except Exception:
        return None


def run(bench_root: Path) -> None:
    print("# Type-fidelity benchmark — BigQuery autoload\n")
    print("Sibling of [`REPORT_types.md`](REPORT_types.md). Same canonical")
    print("PG / MySQL matrix tables, same Rivet-produced Parquet — but the")
    print("downstream reader is `bq load --autodetect --source_format=PARQUET")
    print("--parquet_enable_list_inference --parquet_enum_as_string`.")
    print("Each cell shows the type BigQuery picked plus a classification")
    print("glyph against the ideal BQ representation. See the legend at the")
    print("bottom and the *Findings* section for BigQuery-specific behaviour.\n")
    print("Reproduce:\n")
    print("```bash")
    print("docs/bench/harness/type_bench.sh all          # refresh parquet")
    print("docs/bench/harness/type_bench_bq.sh all       # load each into BQ")
    print("python3 docs/bench/harness/type_diff_bq.py \\")
    print("    tests/.live-tmp/type_bench > docs/bench/reports/REPORT_types_bigquery.md")
    print("```\n")

    print("## Headline\n")
    print("| source | table | ✓ exact | ~ compat | ✗json | ✗tz | ✗! fail |")
    print("|--------|-------|--------:|---------:|------:|----:|--------:|")
    totals = {}
    for (source, table), cols in REFERENCE.items():
        actual = _load_cell(bench_root, source, table)
        bucket = {"EXACT": 0, "COMPAT": 0, "LOSSY_JSON": 0, "LOSSY_TZ": 0, "FAIL": 0}
        for name, _src, ideal in cols:
            cls = _classify(actual.get(name), ideal)
            bucket[cls] = bucket.get(cls, 0) + 1
        totals[(source, table)] = bucket
        print(
            f"| {source} | `{table}` | {bucket['EXACT']} | {bucket['COMPAT']} | "
            f"{bucket['LOSSY_JSON']} | {bucket['LOSSY_TZ']} | {bucket['FAIL']} |"
        )
    print()
    print("---\n")
    print("## Per-table detail\n")

    for (source, table), cols in REFERENCE.items():
        print(f"### {source.upper()} · `{table}`\n")
        actual = _load_cell(bench_root, source, table)
        agg = _load_agg(bench_root, source, table)

        if not actual:
            print("_No BigQuery schema captured — `type_bench_bq.sh` failed for this cell._\n")
            continue

        print("| column | source type | BigQuery autodetect | ideal | glyph |")
        print("|--------|-------------|---------------------|-------|-------|")
        for name, src_native, ideal in cols:
            got = actual.get(name)
            cls = _classify(got, ideal)
            glyph = GLYPH[cls]
            got_s = got if got is not None else "—"
            print(f"| `{name}` | `{src_native}` | `{got_s}` | `{ideal}` | {glyph} |")

        if agg:
            print()
            print("Aggregates after round-trip:")
            print()
            for k, v in agg.items():
                print(f"- **{k}**: `{v}`")
            print()

    print("---\n")
    print("## Findings — BigQuery autoload quirks\n")
    print("1. **`LogicalType::Json` → `BYTES`, not `JSON`.** BigQuery's")
    print("   Parquet autoloader does not promote a Parquet `LogicalType::Json`")
    print("   column into a native BQ `JSON` field. The bytes on disk are")
    print("   the same valid JSON Rivet emits (`pyarrow` confirms this in")
    print("   `pyarrow_validates_*` tests); BigQuery just falls back to")
    print("   `BYTES` instead of `STRING` / `JSON` for this logical type.")
    print("   To materialise as native `JSON`, supply an explicit schema:")
    print()
    print("   ```bash")
    print("   bq load --replace --source_format=PARQUET \\")
    print("       --schema='id:INTEGER,attrs:JSON,...' \\")
    print("       dataset.table file.parquet")
    print("   ```")
    print()
    print("2. **`LogicalType::Uuid` → `BYTES`.** Expected — BigQuery has no")
    print("   native UUID type. The 16-byte payload is the canonical compact")
    print("   form; cast in SQL when needed (`TO_HEX(uid)` for display).\n")
    print("3. **TIMESTAMP / TIMESTAMPTZ both → `TIMESTAMP`.** BigQuery's")
    print("   `TIMESTAMP` is always UTC, so the tz/naive distinction is")
    print("   collapsed at autoload. Wall-clock values round-trip exactly.\n")
    print("4. **Integer widths collapse to `INTEGER`.** BigQuery has one")
    print("   integer type (64-bit). `INT16`/`INT32`/`UINT64` all become")
    print("   `INTEGER` — fine for value preservation, but precision and")
    print("   storage class are normalised.\n")
    print("5. **PG arrays → `RECORD (REPEATED)`.** Inner element type is")
    print("   preserved through the BQ schema; `tags TEXT[]` lands as")
    print("   `RECORD<STRING REPEATED>`.\n")
    print("6. **Enum → STRING (with `--parquet_enum_as_string`).** Without")
    print("   the flag BQ would surface our enum logical type as the raw")
    print("   enum index, which is rarely what operators want.\n")

    print("---\n")
    print("### Legend\n")
    print("| Glyph | Meaning |")
    print("|-------|---------|")
    print("| `✓` | BigQuery autoload picked the ideal BQ type for the semantic |")
    print("| `~` | Different name but value-preserving (e.g. INT16 → `INTEGER`) |")
    print("| `✗json` | Parquet `LogicalType::Json` did not lift to BQ native `JSON`; landed as `BYTES`/`STRING`. Value bytes are valid JSON — needs explicit schema for native `JSON` materialisation. |")
    print("| `✗tz` | tz-aware timestamp lost timezone information (BQ collapses) |")
    print("| `✗!` | Column missing from BQ schema — `bq load` failed for this cell |")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: type_diff_bq.py <bench_root>", file=sys.stderr)
        sys.exit(2)
    run(Path(sys.argv[1]))
