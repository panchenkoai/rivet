#!/usr/bin/env python3
"""Field-shaped A/B: does the fix remove the SYMPTOM, and cost nothing?

    python3 -m dev.pytools.field_replay <old-binary> <new-binary>

The 2026-08-13 regression was not found by a test — it was found by reading a
production run's metadata: 52 exports shed workers, keyset exports ran 2-2.7x
slower than the same tables on the prior release, +1h48m makespan. So the
honest validation is to measure THOSE numbers again, on a workload shaped like
that run, with both binaries.

The shape is reconstructed from the field run's own census (154 exports:
mostly tiny `full` ones, a keyset majority at parallel 1/2/4, a few chunked),
scaled down so a pass/fail lands in minutes. No client data or names are used
— only the distribution of modes and parallelism.

ACCEPTANCE CRITERIA, fixed here before the run so the verdict cannot be
rationalised afterwards:

  1. FIXTURE IS LIVE   the OLD binary must shed at least once. Without this the
                       run reproduces nothing and the rest grades air.
  2. SYMPTOM GONE      the NEW binary sheds ZERO times on an idle source.
  3. NO REGRESSION     new makespan <= old makespan * 1.05.
  4. SAME DATA         every export delivers the same row count on both.

Criterion 1 is the one that makes the others mean anything: it is the
activation guard, and it is checked first.
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

OLD, NEW = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
WORK = Path(os.environ.get("RIVET_FIELD_WORKDIR", "/tmp/rivet-field-replay"))
MYSQL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
POOL_SLOTS = "5"

# (label, count, rows, mode, parallel) — the field census, scaled.
# The heavy keyset shapes are what the regression hit, so they carry the weight.
# NOTE on fidelity, stated rather than glossed: the field's KEYSET exports are
# what the regression hit, but `chunk_by_key:` requires the `table:` shortcut
# (the planner must see a unique index), so a keyset export cannot carry the
# `SELECT DISTINCT` that makes a read provably spill. The spill condition is
# therefore carried by chunked exports at the SAME parallelism — the same
# mechanism the buggy governor fed on (its counter is server-wide, not
# per-runner) — while real keyset exports keep the runner mix in the pool.
CENSUS = [
    ("heavy", 6, 60_000, "spill", 4),
    ("mid", 4, 20_000, "spill", 2),
    ("keyset", 3, 40_000, "keyset", 4),
    ("small", 8, 500, "full", None),
]


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def mysql_root(sql: str) -> str:
    return sh(["docker", "exec", "-i", "rivet-mysql-1", "mysql", "-uroot",
               "-privet", "-N", "rivet"], input=sql).stdout.strip()


def tables():
    for label, n, rows, mode, par in CENSUS:
        for i in range(n):
            yield f"fr_{label}_{i}", rows, mode, par


def seed():
    print("== seeding the field-shaped fixture")
    stmts = ["SET SESSION cte_max_recursion_depth = 1000000;"]
    for name, rows, _, _ in tables():
        stmts.append(f"DROP TABLE IF EXISTS {name};")
        stmts.append(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload VARCHAR(512)) ENGINE=InnoDB;")
        stmts.append(
            f"INSERT INTO {name} (id, payload) WITH RECURSIVE s(n) AS "
            f"(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < {rows}) "
            f"SELECT n, REPEAT('x', 512) FROM s;")
    mysql_root("\n".join(stmts))
    print(f"   {sum(1 for _ in tables())} tables")


class TmpTableGlobals:
    """Force the exports' own reads to spill — the condition the buggy governor
    mistook for source pressure. Restored on exit."""

    def __enter__(self):
        self.prior = mysql_root(
            "SELECT CONCAT_WS(',', @@internal_tmp_mem_storage_engine, "
            "@@tmp_table_size, @@max_heap_table_size);").split(",")
        mysql_root("SET GLOBAL internal_tmp_mem_storage_engine=MEMORY; "
                   "SET GLOBAL tmp_table_size=16384; SET GLOBAL max_heap_table_size=16384;")
        return self

    def __exit__(self, *_):
        e, t, h = self.prior
        mysql_root(f"SET GLOBAL internal_tmp_mem_storage_engine={e}; "
                   f"SET GLOBAL tmp_table_size={t}; SET GLOBAL max_heap_table_size={h};")


def write_config(work: Path, adaptive: bool) -> Path:
    exports = []
    for name, _, mode, par in tables():
        lines = [f"  - name: {name}"]
        if mode == "spill":
            # DISTINCT makes every chunk materialise the derived table, which
            # with the shrunken tmp-table globals provably reaches disk — the
            # own-read exhaust the buggy governor mistook for source pressure.
            lines += [f'    query: "SELECT DISTINCT id, payload FROM {name}"',
                      "    mode: chunked", "    chunk_column: id",
                      "    chunk_size: 10000"]
        elif mode == "keyset":
            lines += [f"    table: {name}", "    mode: chunked",
                      "    chunk_by_key: id", "    chunk_size: 10000"]
        else:
            lines += [f"    table: {name}", "    mode: full"]
        if par:
            lines.append(f"    parallel: {par}")
        lines += ["    format: parquet",
                  f"    destination: {{ type: local, path: {work / 'out' / name} }}"]
        exports.append("\n".join(lines))
    cfg = work / "cfg.yaml"
    cfg.write_text(
        f"source:\n  type: mysql\n  url: \"{MYSQL}\"\n  tuning:\n"
        f"    adaptive: {str(adaptive).lower()}\n    min_parallel: 1\n    batch_size: 500\n"
        f"exports:\n" + "\n".join(exports) + "\n")
    return cfg


def journal_sheds(state_db: Path) -> tuple[int, int]:
    """(backed off, recovered) across every export in the run."""
    import sqlite3
    if not state_db.exists():
        return (0, 0)
    db = sqlite3.connect(state_db)
    back = rec = 0
    for (js,) in db.execute("SELECT journal_json FROM run_journal"):
        for e in json.loads(js).get("entries", []):
            pa = e.get("event", {}).get("ParallelismAdjusted")
            if pa:
                back += "backed off" in pa["reason"]
                rec += "recovered" in pa["reason"]
    return back, rec


def delivered(state_db: Path) -> dict:
    import sqlite3
    if not state_db.exists():
        return {}
    db = sqlite3.connect(state_db)
    return dict(db.execute(
        "SELECT export_name, total_rows FROM export_metrics WHERE status='success'"))


def run(binary: Path, adaptive: bool) -> dict:
    tag = f"{'old' if binary == OLD else 'new'}-{'on' if adaptive else 'off'}"
    work = WORK / tag
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    cfg = write_config(work, adaptive)
    env = dict(os.environ, RUST_LOG="warn", RIVET_GOVERNOR_INTERVAL_MS="200",
               PATH="/usr/bin:/bin:/usr/local/bin")
    t0 = time.monotonic()
    r = sh([str(binary), "apply", str(cfg), "--pool", POOL_SLOTS], env=env, cwd=work)
    wall = time.monotonic() - t0
    back, rec = journal_sheds(work / ".rivet_state.db")
    return {"tag": tag, "exit": r.returncode, "wall": wall,
            "backed_off": back, "recovered": rec, "rows": delivered(work / ".rivet_state.db"),
            "stderr_tail": r.stderr.strip().splitlines()[-4:]}


def main() -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    seed()
    with TmpTableGlobals():
        runs = {r["tag"]: r for r in (
            run(OLD, False), run(OLD, True), run(NEW, False), run(NEW, True))}

    print(f"\n{'run':12}{'exit':>6}{'wall_s':>9}{'backed_off':>12}{'recovered':>11}{'exports':>9}")
    for tag in ("old-off", "old-on", "new-off", "new-on"):
        r = runs[tag]
        print(f"{tag:12}{r['exit']:>6}{r['wall']:>9.1f}{r['backed_off']:>12}"
              f"{r['recovered']:>11}{len(r['rows']):>9}")

    old_on, new_on = runs["old-on"], runs["new-on"]
    verdicts = []
    verdicts.append(("1 fixture is live (old must shed)",
                     old_on["backed_off"] > 0,
                     f"old shed {old_on['backed_off']}x"))
    verdicts.append(("2 symptom gone (new sheds 0 on an idle source)",
                     new_on["backed_off"] == 0,
                     f"new shed {new_on['backed_off']}x"))
    verdicts.append(("3 no makespan regression (new <= old * 1.05)",
                     new_on["wall"] <= old_on["wall"] * 1.05,
                     f"{new_on['wall']:.1f}s vs {old_on['wall']:.1f}s"))
    same = old_on["rows"] == new_on["rows"] and bool(new_on["rows"])
    verdicts.append(("4 same data delivered", same,
                     f"{len(new_on['rows'])} exports, "
                     f"{sum(new_on['rows'].values()):,} rows"))
    # Informational, not a gate: the adaptive-vs-baseline cost on each binary.
    for tag in ("old", "new"):
        on, off = runs[f"{tag}-on"]["wall"], runs[f"{tag}-off"]["wall"]
        print(f"\n{tag}: adaptive ON {on:.1f}s vs OFF {off:.1f}s -> ratio {on / off:.2f}")

    print()
    ok = True
    for name, passed, detail in verdicts:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})")
        ok &= passed
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
