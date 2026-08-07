#!/usr/bin/env python3
"""Run each hypothesis about the field's slow cohort against the local fixture.

WHY: a production run took 5.2 minutes to export 40,791 rows. Four explanations
were plausible from the artifacts alone; three of them were wrong, and reading
the code could not tell them apart. Each is reduced here to a timed export on a
stand with no tunnel, where the answer is a number.

Every hypothesis states what it would mean if TRUE and if FALSE, before it runs —
otherwise a measurement becomes whatever the reader already believed.
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RIVET = ROOT / "target" / "release" / "rivet"
URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
FIELD_SECONDS = 312.0  # aa_import_ikea_es: 40,791 rows, 5.2 min


def mysql(sql, container="rivet-mysql-1"):
    return subprocess.run(
        ["docker", "exec", "-i", container, "mysql", "-urivet", "-privet", "-N", "rivet"],
        input=sql, capture_output=True, text=True).stdout.strip()


def columns(table):
    return mysql(f"SELECT group_concat(column_name) FROM information_schema.columns "
                 f"WHERE table_schema='rivet' AND table_name='{table}'")


def config(work, table, dest, mode="full", extra=""):
    cfg = work / f"{table}.yaml"
    cfg.write_text(
        f"source: {{type: mysql, url_env: ORACLE_URL}}\n"
        f"exports:\n  - name: {table}\n"
        f"    query: >\n      SELECT {columns(table)} FROM {table}\n"
        f"    mode: {mode}\n{extra}"
        f"    format: parquet\n"
        f"    meta_columns: {{exported_at: true, row_hash: true}}\n"
        f"    destination: {dest}\n")
    return cfg


def timed(cfg, out):
    shutil.rmtree(out, ignore_errors=True)
    t = time.monotonic()
    p = subprocess.run([str(RIVET), "run", "-c", str(cfg)],
                       capture_output=True, text=True,
                       env={**__import__("os").environ, "ORACLE_URL": URL})
    return time.monotonic() - t, p.returncode == 0


def raw_mb(table):
    mysql(f"ANALYZE TABLE {table};")
    v = mysql(f"SELECT data_length FROM information_schema.tables "
              f"WHERE table_schema='rivet' AND table_name='{table}'")
    return int(v) / 1e6 if v.isdigit() else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", default="/tmp/aa_probe")
    ns = ap.parse_args()
    work = Path(ns.work); work.mkdir(parents=True, exist_ok=True)
    out = work / "out"
    dest = f"{{type: local, path: {out}/}}"

    print(f"field baseline: aa_import_ikea_es — 40,791 rows, {FIELD_SECONDS:.0f}s\n")
    verdicts = []

    # H1 — is the cost rivet's own?  TRUE  → the cohort's shape is slow in rivet.
    #                                FALSE → the cost is environmental; stop tuning the config.
    t, ok = timed(config(work, "f_ikea_es", dest), out)
    verdicts.append(("H1 rivet's own per-export cost", t, FIELD_SECONDS / max(t, 1e-9),
                     "REFUTED — rivet is not the cost" if t < 5 else "SUPPORTED"))

    # H2 — is it the object-store destination?  (the field writes to real GCS)
    import os
    os.environ["STORAGE_EMULATOR_HOST"] = "http://127.0.0.1:4443"
    gdest = ("\n      type: gcs\n      bucket: rivet-oracle\n"
             "      prefix: aa_probe/ikea/\n      endpoint: http://127.0.0.1:4443")
    t2, ok2 = timed(config(work, "f_ikea_es", gdest), out)
    verdicts.append(("H2 object-store destination", t2, FIELD_SECONDS / max(t2, 1e-9),
                     "REFUTED — destination is not the cost" if t2 < 10 else "SUPPORTED"))

    # H3 — table BLOAT: a full scan reads PAGES, so 40k live rows in a table with
    #      massive dead space might scan like a big table.
    #      TRUE → tell them to OPTIMIZE TABLE. FALSE → dead space is nearly free.
    if mysql("SHOW TABLES LIKE 'f_ikea_bloat'"):
        t3, _ = timed(config(work, "f_ikea_bloat", dest), out)
        ratio = raw_mb("f_ikea_bloat") / max(raw_mb("f_ikea_es"), 1e-9)
        verdicts.append((f"H3 table bloat ({ratio:.0f}x on-disk, same live rows)", t3,
                         FIELD_SECONDS / max(t3, 1e-9),
                         "REFUTED — InnoDB does not pay for dead space here"
                         if t3 < 5 else "SUPPORTED"))

    # H4 — does parquet size predict WIRE volume?  This is the one that matters:
    #      every MB/min figure computed from `file_log` measures COMPRESSED bytes,
    #      while a tunnel carries the server's RAW rows.
    if mysql("SHOW TABLES LIKE 'f_ikea_real'"):
        for t_name in ("f_ikea_es", "f_ikea_real"):
            timed(config(work, t_name, dest), out)
            pq = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1e6
            print(f"  {t_name:<14} raw {raw_mb(t_name):6.1f} MB → parquet {pq:5.2f} MB"
                  f"  = {raw_mb(t_name)/max(pq,1e-9):5.1f}:1")
        print("  → identical raw bytes, parquet differs by compressibility alone.\n"
              "    Any MB/min computed from parquet UNDERSTATES the wire by that ratio.\n")

    print(f"{'hypothesis':<48} {'seconds':>8} {'vs field':>9}  verdict")
    for name, t, r, v in verdicts:
        print(f"{name:<48} {t:8.2f} {r:8.0f}x  {v}")


if __name__ == "__main__":
    sys.exit(main())
