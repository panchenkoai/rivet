"""Two sources, the SAME export name, ONE prefix — what survives?

The concurrency cell races four writers with DISTINCT export names, and the name
is what makes their outputs distinguishable: it is in every part filename
(`{export}_{stamp}_chunk{i}_{nonce}`) and in the run id (`{export}_{stamp}`),
which in turn names the immutable manifest copy. So that test says nothing about
the arrangement an operator falls into by accident: two databases that both have
an `orders` table, two configs that both call the export `orders`, one bucket
prefix.

Everything that distinguishes two runs is derived from the export NAME and a
TIMESTAMP. Give two processes the same name and start them together and the only
thing left separating them is a millisecond. This asks what happens then, on
purpose, and it asks it three ways because the failure modes differ:

    CONCURRENT   started together — the run ids are minted from the same clock
    SEQUENTIAL   one after the other — the second meets the first's `_SUCCESS`
    LOADED       what a warehouse actually ends up holding

THE ORACLE IS THE ID SPACE. The two sources seed DISJOINT ranges (1… and 10001…),
so the expected union is exact and each failure mode is separable: a whole range
absent is an overwrite, a scattered shortfall is loss, a repeated id is
duplication. A row count alone cannot tell the three apart.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RIVET = os.environ.get("RIVET_BIN", str(ROOT / "target" / "release" / "rivet"))
STATE_URL = os.environ.get(
    "RIVET_SN_STATE_URL", "postgresql://rivet:rivet@localhost:5433/rivet_state"
)
BUCKET = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
BQ_PROJECT = os.environ.get("BQ_ORACLE_PROJECT", "rivet-data-tool")
BQ_DATASET = os.environ.get("BQ_ORACLE_DATASET", "rivet_e2e")

TABLE = "same_name_probe"
#: One export NAME for both sources — the whole point.
EXPORT = "orders"
N = 300
SOURCES = [
    ("postgres", "rivet-postgres-1", "postgresql://rivet:rivet@localhost:5432/rivet", 1),
    ("mysql", "rivet-mysql-1", "mysql://rivet:rivet@localhost:3306/rivet", 10_001),
]


def sh(argv, env=None, timeout=900):
    return subprocess.run(
        argv, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout
    )


def duckdb(sql: str) -> str:
    return sh(["duckdb", "-noheader", "-list", "-c", sql], timeout=600).stdout.strip()


def seed() -> bool:
    ok = sh([
        "docker", "exec", "rivet-postgres-1", "psql", "-U", "rivet", "-d", "rivet",
        "-v", "ON_ERROR_STOP=1", "-c",
        f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE}(id INT PRIMARY KEY, src TEXT); "
        f"INSERT INTO {TABLE} SELECT g,'postgres' FROM generate_series(1,{N}) g;",
    ]).returncode == 0
    rows = ",".join(f"({i},'mysql')" for i in range(10_001, 10_001 + N))
    ok2 = subprocess.run(
        ["docker", "exec", "-i", "rivet-mysql-1", "mysql", "-urivet", "-privet", "rivet"],
        input=f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE}(id INT PRIMARY KEY, src TEXT); "
              f"INSERT INTO {TABLE} VALUES {rows};",
        capture_output=True, text=True, timeout=300,
    ).returncode == 0
    return ok and ok2


def config(path: Path, engine: str, url: str, dest: str) -> None:
    # UNQUALIFIED on purpose. A schema-qualified source table (`public.orders`)
    # makes the BigQuery loader read `public` as the DATASET and ignore the
    # configured one — `Not found: Dataset …rivet_e2e.public`. Worth knowing, and
    # not what this probe is asking about, so the name is kept bare exactly as the
    # gate's own BigQuery configs keep it.
    table = TABLE
    path.write_text(
        f'source:\n  type: {engine}\n  url: "{url}"\n'
        f"exports:\n"
        f"  - name: {EXPORT}\n"          # <- IDENTICAL for both sources
        f"    table: {table}\n"
        f"    mode: full\n"
        f"    format: parquet\n"
        f"    destination: {dest}\n"
    )


def judge(label: str, ids: list[int], copies: int, runs: int) -> list[str]:
    expected = set(range(1, N + 1)) | set(range(10_001, 10_001 + N))
    seen = set(ids)
    problems = []
    missing = expected - seen
    if missing:
        pg = len([i for i in missing if i <= N])
        my = len(missing) - pg
        problems.append(
            f"{len(missing)} row(s) absent (postgres {pg}, mysql {my}) — a whole range gone is "
            f"one source's output OVERWRITTEN by the other's, a scatter is loss"
        )
    if len(ids) != len(seen):
        problems.append(f"{len(ids) - len(seen)} DUPLICATE id(s) — a load over this prefix double-counts")
    if copies != runs:
        problems.append(
            f"{copies} run-unique manifest copies for {runs} runs — the copies are named from the "
            f"run id, which is the export name plus a timestamp, so two runs of one NAME can "
            f"collide on it and a whole run becomes invisible to a manifest-summing consumer"
        )
    return problems


def local_leg(work: Path, concurrent: bool) -> tuple[str, list[str], list[str]]:
    label = "concurrent" if concurrent else "sequential"
    dest = work / label
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)
    cfgs = []
    for engine, _c, url, _b in SOURCES:
        p = work / f"{label}_{engine}.yaml"
        config(p, engine, url, f"{{ type: local, path: {dest} }}")
        cfgs.append(p)

    env = {"RIVET_STATE_URL": STATE_URL}
    if concurrent:
        procs = [subprocess.Popen([RIVET, "run", "-c", str(c)], stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True,
                                  env={**os.environ, **env}) for c in cfgs]
        outs = [p.communicate()[0] for p in procs]
        exits = [p.returncode for p in procs]
    else:
        outs, exits = [], []
        for c in cfgs:
            r = sh([RIVET, "run", "-c", str(c)], env=env)
            outs.append(r.stdout)
            exits.append(r.returncode)

    notes = []
    if any(e != 0 for e in exits):
        tail = [o.strip().splitlines()[-1] if o.strip() else "" for o in outs]
        return label, [f"writer exits {exits}: {tail}"], notes
    parts = sorted(dest.rglob("*.parquet"))
    copies = sorted(dest.rglob("manifest-*.json"))
    ids = []
    if parts:
        lst = ",".join(f"'{p}'" for p in parts)
        ids = [int(x) for x in duckdb(f"SELECT id FROM read_parquet([{lst}])").splitlines()
               if x.strip().lstrip("-").isdigit()]
    notes.append(f"{len(parts)} part(s), {len(copies)} manifest copies, {len(set(ids))} distinct ids")
    ru = {json.loads(m.read_text()).get("run_id") for m in copies}
    notes.append(f"run ids: {sorted(ru)}")
    return label, judge(label, ids, len(copies), 2), notes


def bigquery_leg(work: Path) -> tuple[str, list[str], list[str]]:
    label = "gcs+bigquery"
    notes: list[str] = []
    if not shutil.which("gsutil") or not shutil.which("bq"):
        return label, [], ["gsutil/bq absent — skipped"]
    pfx = f"same-name-{time.strftime('%H%M%S')}"
    table = f"{EXPORT}_{time.strftime('%H%M%S')}"
    cfgs = []
    for engine, _c, url, _b in SOURCES:
        p = work / f"bq_{engine}.yaml"
        config(p, engine, url, f"{{ type: gcs, bucket: {BUCKET}, prefix: {pfx}/ }}")
        # The load block turns this into "what does the warehouse end up holding".
        p.write_text(p.read_text().replace(f"  - name: {EXPORT}\n", f"  - name: {table}\n")
                     + f"load:\n  target: bigquery\n  project: {BQ_PROJECT}\n  dataset: {BQ_DATASET}\n")
        cfgs.append(p)

    env = {"RIVET_STATE_URL": STATE_URL}
    procs = [subprocess.Popen([RIVET, "run", "-c", str(c)], stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True,
                              env={**os.environ, **env}) for c in cfgs]
    outs = [p.communicate()[0] for p in procs]
    exits = [p.returncode for p in procs]
    if any(e != 0 for e in exits):
        return label, [f"writer exits {exits}: {[o.strip().splitlines()[-1:] for o in outs]}"], notes

    listing = sh(["gsutil", "ls", f"gs://{BUCKET}/{pfx}/**"], timeout=300).stdout.splitlines()
    objs = [o.strip() for o in listing if o.strip().startswith("gs://")]
    parts = [o for o in objs if o.endswith(".parquet")]
    copies = [o for o in objs if "/manifest-" in o]
    notes.append(f"{len(parts)} part(s), {len(copies)} manifest copies on gs://{BUCKET}/{pfx}/")

    # BOTH loads, because that is what an operator with two configs does — and
    # the warehouse table is named from the SOURCE TABLE, not from the export
    # name, so both land in one table and the question is what it ends up holding.
    for i, c in enumerate(cfgs):
        load = sh([RIVET, "load", "-c", str(c)], env=env, timeout=1800)
        tail = [ln for ln in (load.stdout + load.stderr).strip().splitlines() if "warehouse" in ln]
        notes.append(
            f"load[{SOURCES[i][0]}] exit {load.returncode}: "
            f"{tail[-1].strip() if tail else (load.stdout + load.stderr).strip().splitlines()[-1:]}"
        )
    q = sh(["bq", "--project_id", BQ_PROJECT, "query", "--nouse_legacy_sql", "--format=csv",
            f"SELECT count(*) AS n, count(DISTINCT id) AS d FROM `{BQ_PROJECT}.{BQ_DATASET}.{TABLE}`"],
           timeout=600).stdout.strip().splitlines()
    problems: list[str] = []
    if len(q) >= 2 and "," in q[1]:
        n, d = (int(x) for x in q[1].split(","))
        notes.append(f"BigQuery holds {n} rows, {d} distinct ids (expected {2 * N} / {2 * N})")
        if n != 2 * N or d != 2 * N:
            problems.append(
                f"BigQuery holds {n} rows / {d} distinct ids, both should be {2 * N} — "
                f"n>d is duplication, d<{2 * N} is loss or overwrite"
            )
    else:
        problems.append(f"could not read the loaded table back: {q}")
    sh(["gsutil", "-m", "rm", "-r", f"gs://{BUCKET}/{pfx}"], timeout=600)
    sh(["bq", "--project_id", BQ_PROJECT, "rm", "-f", "-t", f"{BQ_DATASET}.{TABLE}"], timeout=300)
    return label, problems, notes


def main() -> int:
    if not Path(RIVET).exists():
        print(f"binary not found: {RIVET}")
        return 2
    work = Path(os.environ.get("RIVET_SN_WORK", "/tmp/rivet_same_name"))
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    print(f"=== two sources, ONE export name `{EXPORT}`, ONE prefix ===")
    if not seed():
        print("  could not seed both sources")
        return 2
    print(f"  postgres ids 1..{N}, mysql ids 10001..{10_000 + N} — disjoint on purpose\n")

    results = [local_leg(work, concurrent=True), local_leg(work, concurrent=False)]
    if os.environ.get("RIVET_SN_CLOUD", "1") == "1":
        results.append(bigquery_leg(work))

    bad = 0
    for label, problems, notes in results:
        for n in notes:
            print(f"  [{label}] {n}")
        if problems:
            bad += 1
            print(f"  ✗ {label}: " + "; ".join(problems))
        else:
            print(f"  ✓ {label}: the union is exact — no loss, no duplicate, no overwrite")
        print()
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
