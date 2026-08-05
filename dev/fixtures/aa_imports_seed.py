#!/usr/bin/env python3
"""Recreate the field's `aa_import_*` cohort locally, by MEASURED shape.

WHY THIS EXISTS

A production run took 3h51m to move 1.48 GB, and twelve exports of 2.4–8.0 MB
each took 4.8–5.5 minutes — time nearly constant while the data varied 3x. The
harm counters that looked like they explained it (`Innodb_rows_read`) turned out
to be deltas of a GLOBAL server counter taken around CONCURRENT exports, so they
attributed every other export's work to whichever one was being measured. rivet's
own doc comment says so: "accurate on a quiet pilot box". They were read as
per-export truth and produced a confident, wrong diagnosis.

So the shapes are reproduced here instead, on a stand with no tunnel and no
concurrent load, where the same question can be asked of something measurable.

WHAT IS REPRODUCED, AND FROM WHERE

Every number below is measured from the field artifacts, not invented:
  rows        `export_metrics.total_rows` (what the run actually delivered)
  columns     the curated SELECT in the config `rivet init` generated
  bytes/row   `file_log.bytes / file_log.row_count` — real compressed width
  key shape   `rivet check`'s "Cursor range" vs the row estimate, which is what
              proves `ref_id` is NOT unique (15.9M rows over a span of 7.36M =
              2.16 rows per key, so keyset is impossible on it)
  index       `rivet check`'s "Access:" line

The big tables are scaled DOWN by row count but keep their key DENSITY and
uniqueness, because those are what decide the strategy. The small ones are exact
— they are the unexplained cohort, and they are cheap.
"""

import argparse
import random
import subprocess
import sys

# (table, rows, ncols, bytes_per_row_compressed, key, key_density, indexed)
#
# `key_density` is rows per distinct key value: 1.0 means the key is unique and
# keyset works; >1 means it is not, and the planner will refuse `chunk_by_key`.
COHORT = [
    # the unexplained cohort — exact sizes, no index on the read path
    ("f_ikea_es",        40_791, 16,  78, None, 0,    False),
    ("f_iherbs",         37_265, 17,  84, None, 0,    False),
    ("f_shopee_in",      44_815, 20,  88, None, 0,    False),
    ("f_actionpays",     73_023, 16,  94, None, 0,    False),
    ("f_amazon_pt",      84_693, 20,  94, None, 0,    False),
    # the one that CROSSED the 300s ceiling: biggest of the family
    ("f_partnerize",     94_000, 17,  90, None, 0,    False),
    # controls that WORK in the field: unique id, index scan, ~870k rows/min
    ("f_bonus_conv_eur", 500_000, 12, 19, "id",  1.0,  True),
    # the range victims, scaled 30x down, DENSITY preserved (non-unique key)
    ("f_bonuses_version", 500_000, 16, 22, "ref_id", 2.16, True),
    ("f_amazon_in_ver",   200_000, 11, 24, "ref_id", 5.24, True),
]


def ddl(name, ncols, brow, key, indexed):
    """A table whose ROW WIDTH matches the field's, in the field's column count.

    Width matters more than the exact types: the question is how long rivet takes
    per byte and per export, and a 16-column table of the right width answers it.
    The parquet figure is COMPRESSED, so the raw column is sized generously —
    text compresses roughly 3-5x at zstd, and under-sizing here would make the
    fixture easier than the thing it reproduces.
    """
    raw = max(brow * 4, 40)
    cols = []
    if key:
        cols.append(f"`{key}` bigint unsigned NOT NULL")
    cols.append("`status` varchar(32) NULL")
    cols.append("`amount` decimal(18,4) NULL")
    cols.append("`created_at` datetime NULL")
    cols.append("`updated_at` datetime NULL")
    # spread the remaining width across text columns, like the import payloads
    rest = ncols - len(cols)
    per = max(raw // max(rest, 1), 16)
    for i in range(rest):
        cols.append(f"`c{i}` varchar({min(per, 512)}) NULL")
    idx = f", KEY `k_{key}` (`{key}`)" if (key and indexed) else ""
    return f"CREATE TABLE `{name}` (\n  " + ",\n  ".join(cols) + idx + "\n) ENGINE=InnoDB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", default="rivet-mysql-1")
    ap.add_argument("--db", default="rivet")
    ap.add_argument("--only", default="", help="comma-separated subset")
    ns = ap.parse_args()

    want = set(x for x in ns.only.split(",") if x)
    sql = []
    for name, rows, ncols, brow, key, dens, indexed in COHORT:
        if want and name not in want:
            continue
        sql.append(f"DROP TABLE IF EXISTS `{name}`;")
        sql.append(ddl(name, ncols, brow, key, indexed) + ";")

    run(ns, "\n".join(sql))
    print("tables created", file=sys.stderr)

    for name, rows, ncols, brow, key, dens, indexed in COHORT:
        if want and name not in want:
            continue
        fill(ns, name, rows, ncols, brow, key, dens)
        print(f"  {name}: {rows:,} rows", file=sys.stderr)


def fill(ns, name, rows, ncols, brow, key, dens):
    """Generate rows server-side from a recursive CTE — no round-trips.

    A client-side INSERT loop over 500k rows spends its time in the driver, which
    is exactly the confound this fixture exists to remove.
    """
    raw = max(brow * 4, 40)
    ncols_text = ncols - (5 if key else 4)
    per = max(raw // max(ncols_text, 1), 16)
    # `dens` rows per key value: FLOOR((n-1)/dens)+1 repeats each key `dens` times
    keyexpr = f"FLOOR((n - 1) / {dens}) + 1" if key and dens > 1 else "n"
    sel = []
    if key:
        sel.append(f"{keyexpr}")
    sel += ["ELT(1 + (n % 4), 'new', 'done', 'hold', 'void')",
            "ROUND((n % 100000) + 0.99, 2)",
            "DATE_SUB(NOW(), INTERVAL (n % 900) DAY)",
            "DATE_SUB(NOW(), INTERVAL (n % 400) DAY)"]
    for i in range(ncols_text):
        sel.append(f"RPAD(CONCAT('c{i}_', n), {min(per,512)}, 'x')")
    body = ", ".join(sel)
    # chunked so one statement never builds a 500k-row temp result
    step = 900  # under the default cte_max_recursion_depth (1000)
    for lo in range(0, rows, step):
        hi = min(lo + step, rows)
        run(ns, f"""
INSERT INTO `{name}`
WITH RECURSIVE s(n) AS (  /* depth is bounded by `step` below, so the
     server default (1000) is raised per statement instead of per session:
     the seed user has no SESSION_VARIABLES_ADMIN on a least-privilege stand */
  SELECT {lo + 1} UNION ALL SELECT n + 1 FROM s WHERE n < {hi}
)
SELECT {body} FROM s;""")


def run(ns, sql):
    p = subprocess.run(
        ["docker", "exec", "-i", ns.container, "mysql", "-urivet", "-privet", ns.db],
        input=sql, capture_output=True, text=True,
    )
    if p.returncode != 0:
        print(p.stderr[-800:], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
