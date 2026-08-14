#!/usr/bin/env python3
"""Differential A/B: does the new binary still behave like the released one?

    python3 -m dev.pytools.ab_regression <old-binary> <new-binary>

`governor_ab.py` grades ONE mechanism and asks "is the fix in?". This asks the
complementary question the fix rounds cannot answer about themselves — "did we
break anything that used to work?" — by running both binaries over identical
fixtures and diffing everything a user can observe.

Four bughunt rounds changed the governor, the pool predictor, the harm
bracket, the retry counter, the plan output and the apply path. Every one of
those was verified to FIX something. This asks the opposite question — did any
of them BREAK something that used to work?

Method: run both binaries over identical fixtures and diff the observable
surface a user depends on: exit code, delivered rows (read back by DuckDB, not
by rivet), file count, and the manifest's own accounting. Volatile fields
(run_id, timestamps, paths, durations) are normalised away; anything else that
differs is reported for judgement rather than silently tolerated.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

OLD = Path(sys.argv[1]).resolve()
NEW = Path(sys.argv[2]).resolve()
ROOT = Path(os.environ.get("RIVET_AB_WORKDIR", "/tmp/rivet-ab-regression"))
PG = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
MYSQL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def pg(sql):
    return sh(["docker", "exec", "-i", "rivet-postgres-1", "psql", "-U", "rivet",
               "-d", "rivet", "-tAc", sql]).stdout.strip()


def duckdb_rows(d: Path) -> int:
    if not any(d.rglob("*.parquet")):
        return 0
    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"SELECT count(*) FROM read_parquet('{d}/**/*.parquet');"])
    try:
        return int(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return -1


def seed():
    pg("DROP TABLE IF EXISTS ab_src;"
       "CREATE TABLE ab_src (id bigint PRIMARY KEY, payload text, updated_at timestamptz DEFAULT now());"
       "INSERT INTO ab_src (id, payload) SELECT g, repeat('x', 256) FROM generate_series(1, 50000) g;")


SCENARIOS = {
    # name: (yaml body template, expected_rows)
    "full": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    table: ab_src
    mode: full
    format: parquet
    destination: {{ type: local, path: {out} }}
""", 50000),
    "chunked_parallel": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500 }}
exports:
  - name: ab
    table: ab_src
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 4
    format: parquet
    destination: {{ type: local, path: {out} }}
""", 50000),
    "keyset_parallel": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500 }}
exports:
  - name: ab
    table: ab_src
    mode: chunked
    chunk_by_key: id
    chunk_size: 5000
    parallel: 4
    format: parquet
    destination: {{ type: local, path: {out} }}
""", 50000),
    # The runner this work MODIFIED — the highest regression risk in the diff.
    "chunk_checkpoint": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500, min_parallel: 1 }}
exports:
  - name: ab
    table: ab_src
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 4
    chunk_checkpoint: true
    format: parquet
    destination: {{ type: local, path: {out} }}
""", 50000),
    "incremental": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    query: "SELECT id, payload, updated_at FROM ab_src"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{ type: local, path: {out} }}
""", 50000),
    "csv_format": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    table: ab_src
    mode: full
    format: csv
    destination: {{ type: local, path: {out} }}
""", None),  # rows checked via file presence only
}


def run_case(binary: Path, name: str, body: str, extra_args=None) -> dict:
    work = ROOT / name / ("old" if binary == OLD else "new")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out = work / "out"
    cfg = work / "cfg.yaml"
    cfg.write_text(body.format(pg=PG, out=out))
    env = dict(os.environ, RUST_LOG="warn", PATH="/usr/bin:/bin:/usr/local/bin")
    args = [str(binary), "run", "-c", str(cfg)] + (extra_args or [])
    r = sh(args, env=env, cwd=work)
    manifest = None
    for m in out.rglob("manifest.json"):
        try:
            j = json.loads(m.read_text())
            parts = j.get("parts", [])
            manifest = {
                "status": j["status"],
                "row_count": j["row_count"],
                "part_count": j["part_count"],
                "parts_rows": sum(p["rows"] for p in parts),
                # Content fingerprints are the strongest cross-binary oracle
                # available: identical source, identical rows, identical
                # encoder settings => identical digests. A change here means
                # the BYTES differ, not just the accounting.
                "fingerprints": sorted(p["content_fingerprint"] for p in parts),
                "column_checksums": j.get("column_checksums"),
                "schema_fingerprint": j["schema_fingerprint"],
                "keys": sorted(j.keys()),
            }
        except Exception as e:  # noqa: BLE001
            manifest = {"parse_error": str(e)}
        break
    return {
        "exit": r.returncode,
        "rows_readback": duckdb_rows(out),
        "files": len(list(out.rglob("*.parquet"))) or len(list(out.rglob("*.csv"))),
        "manifest": manifest,
        "stderr_tail": r.stderr.strip().splitlines()[-3:] if r.returncode else [],
    }



def crash_resume_case(binary: Path) -> dict:
    """The riskiest change in the diff: the chunk_checkpoint runner's claim loop
    now takes a governor permit per task. Crash it mid-run, resume, and compare
    what the two binaries recover — this is the path where a mis-scoped permit
    would strand a task as `running` forever or lose a chunk."""
    work = ROOT / "crash_resume" / ("old" if binary == OLD else "new")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out, cfg = work / "out", work / "cfg.yaml"
    cfg.write_text(SCENARIOS["chunk_checkpoint"][0].format(pg=PG, out=out))
    base = dict(os.environ, RUST_LOG="warn", PATH="/usr/bin:/bin:/usr/local/bin")
    # 1) crash after the 3rd chunk file lands
    crash = sh([str(binary), "run", "-c", str(cfg)],
               env=dict(base, RIVET_TEST_PANIC_AT="after_chunk_file:3"), cwd=work)
    mid_files = len(list(out.rglob("*.parquet")))
    # 2) resume
    res = sh([str(binary), "run", "-c", str(cfg), "--resume"], env=base, cwd=work)
    tasks = sh(["python3", "-c", (
        "import sqlite3,sys;d=sqlite3.connect(sys.argv[1]);"
        "print(sorted((s,n) for s,n in d.execute("
        "'SELECT status, count(*) FROM chunk_task GROUP BY status')))"
    ), str(work / ".rivet_state.db")]).stdout.strip()
    # Raw parquet under a crashed prefix includes the un-manifested orphan the
    # crashed attempt left behind — counting it is the "artifacts are not
    # evidence" trap. Report BOTH: the manifest's delivered rows (the claim a
    # consumer reads) and the raw count (which must match on both binaries too,
    # since a changed orphan count is itself a behaviour change).
    delivered = None
    for m in out.rglob("manifest.json"):
        j = json.loads(m.read_text())
        delivered = (j["status"], j["row_count"], j["part_count"])
        break
    return {
        "crash_exit": crash.returncode,
        "files_after_crash": mid_files,
        "resume_exit": res.returncode,
        "manifest_delivered": delivered,
        "raw_parquet_rows": duckdb_rows(out),
        "chunk_task_states": tasks,
    }


def failure_and_plan_cases(binary: Path) -> dict:
    """Two surfaces this work touched directly: the error path's exit code, and
    `plan --format json` stdout, which must be ONE parseable document (the pool
    advisory started leaking into it on this branch and was fixed)."""
    work = ROOT / "surfaces" / ("old" if binary == OLD else "new")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    base = dict(os.environ, RUST_LOG="error", PATH="/usr/bin:/bin:/usr/local/bin")

    blocker = work / "blocker"
    blocker.write_text("not a directory")
    bad = work / "bad.yaml"
    bad.write_text(SCENARIOS["full"][0].format(pg=PG, out=blocker / "sub"))
    fail = sh([str(binary), "run", "-c", str(bad)], env=base, cwd=work)

    multi = work / "multi.yaml"
    multi.write_text(f"""
source: {{ type: postgres, url: "{PG}" }}
exports:
  - name: ab_one
    table: ab_src
    mode: full
    format: parquet
    destination: {{ type: local, path: {work / 'o1'} }}
  - name: ab_two
    table: ab_src
    mode: full
    format: parquet
    destination: {{ type: local, path: {work / 'o2'} }}
""")
    plan = sh([str(binary), "plan", "--config", str(multi), "--format", "json"],
              env=base, cwd=work)
    try:
        doc = json.loads(plan.stdout)
        plan_shape = f"valid json, {type(doc).__name__}[{len(doc)}]"
    except Exception as e:  # noqa: BLE001
        plan_shape = f"UNPARSEABLE: {e}"
    return {
        "fail_exit": fail.returncode,
        "plan_exit": plan.returncode,
        "plan_stdout": plan_shape,
    }


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    seed()
    print(f"{'scenario':20} {'field':16} {'old (0.24.4)':>22} {'new':>22}  verdict")
    print("=" * 96)
    regressions = []
    for name, (body, expect) in SCENARIOS.items():
        a = run_case(OLD, name, body)
        b = run_case(NEW, name, body)
        for field in ("exit", "rows_readback", "files"):
            same = a[field] == b[field]
            verdict = "same" if same else "DIFFERS"
            if not same:
                regressions.append((name, field, a[field], b[field]))
            print(f"{name:20} {field:16} {str(a[field]):>22} {str(b[field]):>22}  {verdict}")
        ma, mb = a["manifest"], b["manifest"]
        if ma != mb:
            regressions.append((name, "manifest", ma, mb))
            print(f"{name:20} {'manifest':16} {str(ma)[:22]:>22} {str(mb)[:22]:>22}  DIFFERS")
        else:
            shape = f"rows={ma['row_count']} parts={ma['part_count']}" if ma else "none"
            print(f"{name:20} {'manifest':16} {shape:>22} {shape:>22}  same")
        if expect is not None and b["rows_readback"] != expect:
            regressions.append((name, "expected_rows", expect, b["rows_readback"]))
    for label, fn in (("crash_resume", crash_resume_case), ("surfaces", failure_and_plan_cases)):
        a, b = fn(OLD), fn(NEW)
        for k in a:
            same = a[k] == b[k]
            if not same:
                regressions.append((label, k, a[k], b[k]))
            print(f"{label:20} {k:16} {str(a[k])[:22]:>22} {str(b[k])[:22]:>22}  {'same' if same else 'DIFFERS'}")
    print("=" * 96)
    if regressions:
        print(f"\n{len(regressions)} DIFFERENCE(S) — each needs a verdict:")
        for r in regressions:
            print(" ", r)
        return 1
    print("\nno observable difference across", len(SCENARIOS) + 2, "scenarios")
    return 0


if __name__ == "__main__":
    sys.exit(main())
