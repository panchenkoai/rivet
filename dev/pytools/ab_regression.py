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

════════════════════════════════════════════════════════════════════════════
Running it as a RELEASE-GATE step (what the hardening pass added, and why)
════════════════════════════════════════════════════════════════════════════

None of this changes what the harness asserts about old-vs-new. It changes
what happens when the harness cannot assert anything, which is the failure
mode a gate actually meets.

* **Preflight, before a single row is seeded.** The harness needs docker, a
  RUNNING and ACCEPTING Postgres publishing the URL's port, `duckdb` on PATH,
  and two distinct executable binaries that answer `--version`. Each of those
  absent produced a different confusing symptom (a traceback from
  `FileNotFoundError: duckdb`, an empty psql answer, a `rows_readback` of -1 on
  both sides that compared equal and PASSED). `preflight` names what is missing
  and refuses to start.

* **Concurrency: NAMESPACE, not a lock.** Every fixture this harness owns is
  suffixed with a per-run tag (`ab_src_<pid>` and a workdir under it), because
  nothing it asserts is perturbed by a co-tenant: it makes no timing claim, and
  each run reads only its own table into its own output. Two of these can run
  side by side, which is what you want of a step a developer may need to
  re-run while a release job holds the stand. The MySQL sibling
  (`field_replay.py`) takes an exclusive LOCK instead, for the two reasons that
  do not apply here: it mutates SERVER-WIDE globals, and its verdict includes a
  wall-clock comparison that a co-tenant invalidates. The table is dropped on
  the way out; a namespaced fixture that leaks is a worse mess than a fixed one.

* **A cell that graded NOTHING fails; a cell that graded PART of itself warns.**
  Both sides producing zero output files compared two empty runs and read as
  "no observable difference" — the vacuous green this whole session is about.
  Same for a crash case where the injected crash never fired, and an error-path
  case where the run that must fail exited 0. Those FAIL. Weaker gaps (no
  manifest to compare, DuckDB unable to read either side, an unparseable plan
  on both) are printed under a loud banner rather than swallowed.

* **State isolation is forced.** `RIVET_STATE_URL=""` is set for every child, so
  a caller that exports a shared Postgres state DB (the release oracle does,
  process-wide) cannot (a) hide the `chunk_task` readback this harness needs —
  it reads `.rivet_state.db` beside the config — or (b) make the OLD binary
  refuse a schema the NEW one just migrated, which is the documented
  cross-version state trap in `dev/release_oracle/regression.py`.

* **A manifest difference names the differing KEY**, one line per key, instead
  of two 4 KB dict blobs the reader has to diff by eye.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path

from .shell import Fail, have, main as shell_main, run, tcp_open

# ── the stand this harness talks to ────────────────────────────────────────────
PG = os.environ.get("RIVET_AB_PG_URL", "postgresql://rivet:rivet@127.0.0.1:5432/rivet")
PG_CONTAINER = os.environ.get("RIVET_AB_PG_CONTAINER", "rivet-postgres-1")
SEED_ROWS = 50_000

# ── per-run namespace (see the "Concurrency" note above) ───────────────────────
_TAG = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("RIVET_AB_TAG") or str(os.getpid()))
TABLE = f"ab_src_{_TAG}"
# An explicitly-set workdir is honoured verbatim (the operator chose it); the
# default gets the tag, so two default runs cannot rmtree each other's output.
ROOT = Path(os.environ.get("RIVET_AB_WORKDIR") or f"/tmp/rivet-ab-regression/{_TAG}")

# Long enough that a genuinely slow binary is not mistaken for a hung one, short
# enough that a hang ends the step instead of the CI job. A timeout surfaces as
# returncode 124, i.e. as a DIFFERENCE, never as a quiet pass.
CASE_TIMEOUT = float(os.environ.get("RIVET_AB_TIMEOUT") or 900)

# Empty means "SQLite beside the config" — see the state-isolation note above.
# `run()` merges over os.environ, so CLEARING an inherited value needs the empty
# string, not a missing key.
CHILD_ENV = {"RUST_LOG": "warn", "PATH": "/usr/bin:/bin:/usr/local/bin", "RIVET_STATE_URL": ""}


def sh(cmd, **kw):
    """A process, with a timeout. `Proc` exposes the same .returncode/.stdout/
    .stderr as `subprocess.CompletedProcess`, so call sites read unchanged."""
    kw.setdefault("timeout", CASE_TIMEOUT)
    return run(cmd, **kw)


def pg(sql, *, check: bool = True) -> str:
    p = sh(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "rivet", "-d", "rivet",
            "-v", "ON_ERROR_STOP=1", "-tAc", sql], timeout=600)
    if check and not p.ok:
        tail = (p.stderr or p.stdout).strip().splitlines()
        raise Fail(f"psql failed on {PG_CONTAINER}: {tail[-1] if tail else p.returncode}",
                   hint=f"SQL was: {sql[:200]}")
    return p.stdout.strip()


def duckdb_rows(d: Path) -> int:
    """Rows an INDEPENDENT reader sees. -1 means DuckDB could not read them at
    all — kept distinct from 0 (nothing written), because the two are different
    findings and only one of them is a fixture problem."""
    if not any(d.rglob("*.parquet")):
        return 0
    r = sh(["duckdb", "-noheader", "-list", "-c",
            f"SELECT count(*) FROM read_parquet('{d}/**/*.parquet');"], timeout=600)
    try:
        return int(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return -1


def seed():
    pg(f"DROP TABLE IF EXISTS {TABLE};"
       f"CREATE TABLE {TABLE} (id bigint PRIMARY KEY, payload text, updated_at timestamptz DEFAULT now());"
       f"INSERT INTO {TABLE} (id, payload) SELECT g, repeat('x', 256) FROM generate_series(1, {SEED_ROWS}) g;")
    # A seed that half-worked would surface later as a row-count difference
    # attributed to the BINARIES. Check it here, where it is still a fixture bug.
    got = pg(f"SELECT count(*) FROM {TABLE};")
    if got != str(SEED_ROWS):
        raise Fail(f"fixture seed wrote {got!r} rows into {TABLE}, expected {SEED_ROWS}")
    print(f"== fixture {TABLE} seeded ({SEED_ROWS} rows) on {PG_CONTAINER}; workdir {ROOT}")


SCENARIOS = {
    # name: (yaml body template, expected_rows)
    "full": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    table: {table}
    mode: full
    format: parquet
    destination: {{ type: local, path: {out} }}
""", SEED_ROWS),
    "chunked_parallel": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500 }}
exports:
  - name: ab
    table: {table}
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 4
    format: parquet
    destination: {{ type: local, path: {out} }}
""", SEED_ROWS),
    "keyset_parallel": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500 }}
exports:
  - name: ab
    table: {table}
    mode: chunked
    chunk_by_key: id
    chunk_size: 5000
    parallel: 4
    format: parquet
    destination: {{ type: local, path: {out} }}
""", SEED_ROWS),
    # The runner this work MODIFIED — the highest regression risk in the diff.
    "chunk_checkpoint": ("""
source:
  type: postgres
  url: "{pg}"
  tuning: {{ adaptive: true, batch_size: 500, min_parallel: 1 }}
exports:
  - name: ab
    table: {table}
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 4
    chunk_checkpoint: true
    format: parquet
    destination: {{ type: local, path: {out} }}
""", SEED_ROWS),
    "incremental": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    query: "SELECT id, payload, updated_at FROM {table}"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{ type: local, path: {out} }}
""", SEED_ROWS),
    "csv_format": ("""
source: {{ type: postgres, url: "{pg}" }}
exports:
  - name: ab
    table: {table}
    mode: full
    format: csv
    destination: {{ type: local, path: {out} }}
""", None),  # rows checked via file presence only
}


def _case_dir(name: str, side: str) -> Path:
    """One directory per (case, side). `side` is passed in rather than derived
    from the binary path so that pointing the harness at the SAME binary twice
    (the harness's own self-test: expect zero differences) still gives each leg
    its own workdir instead of the second silently overwriting the first."""
    work = ROOT / name / side
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    return work


def run_case(binary: Path, side: str, name: str, body: str, extra_args=None) -> dict:
    work = _case_dir(name, side)
    out = work / "out"
    cfg = work / "cfg.yaml"
    cfg.write_text(body.format(pg=PG, out=out, table=TABLE))
    args = [str(binary), "run", "-c", str(cfg)] + (extra_args or [])
    r = sh(args, env=CHILD_ENV, cwd=work)
    manifest = None
    manifests = sorted(out.rglob("manifest.json"))
    for m in manifests[:1]:
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
    return {
        "exit": r.returncode,
        "rows_readback": duckdb_rows(out),
        "files": len(list(out.rglob("*.parquet"))) or len(list(out.rglob("*.csv"))),
        "manifest": manifest,
        "manifest_files": len(manifests),
        "stderr_tail": r.stderr.strip().splitlines()[-3:] if r.returncode else [],
    }


# ── manifest diffing ───────────────────────────────────────────────────────────
def _brief(v, limit: int = 46) -> str:
    s = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return s if len(s) <= limit else s[: limit - 1] + "…"


def manifest_diff(a, b) -> list[tuple[str, str, str]]:
    """Per-KEY differences as (key, old, new).

    The whole point is that a failure NAMES the field. Comparing the two dicts
    and printing them is what this replaced: a future manifest that gains one
    field produced two unreadable blobs and left the reader to spot the addition
    by eye. Lists and nested dicts are summarised by WHAT differs (how many
    entries, which index, which column) rather than dumped.
    """
    if a == b:
        return []
    if a is None or b is None:
        return [("<manifest>", "absent" if a is None else "present",
                 "absent" if b is None else "present")]
    out: list[tuple[str, str, str]] = []
    for k in sorted(set(a) | set(b)):
        va, vb = a.get(k, "<key absent>"), b.get(k, "<key absent>")
        if va == vb:
            continue
        if k == "keys" and isinstance(va, list) and isinstance(vb, list):
            # The "a future manifest gains a field" case, said in one line.
            out.append((k, f"-{sorted(set(va) - set(vb))}", f"+{sorted(set(vb) - set(va))}"))
        elif isinstance(va, list) and isinstance(vb, list):
            if len(va) != len(vb):
                out.append((f"{k}[len]", str(len(va)), str(len(vb))))
            for i, (x, y) in enumerate(zip(va, vb)):
                if x != y:
                    out.append((f"{k}[{i}]", _brief(x), _brief(y)))
                    break  # the first differing entry is the actionable one
        elif isinstance(va, dict) and isinstance(vb, dict):
            for ck in sorted(set(va) | set(vb)):
                if va.get(ck, "<absent>") != vb.get(ck, "<absent>"):
                    out.append((f"{k}.{ck}", _brief(va.get(ck, "<absent>")),
                                _brief(vb.get(ck, "<absent>"))))
        else:
            out.append((k, _brief(va), _brief(vb)))
    return out or [("<manifest>", _brief(a), _brief(b))]


def crash_resume_case(binary: Path, side: str) -> dict:
    """The riskiest change in the diff: the chunk_checkpoint runner's claim loop
    now takes a governor permit per task. Crash it mid-run, resume, and compare
    what the two binaries recover — this is the path where a mis-scoped permit
    would strand a task as `running` forever or lose a chunk."""
    work = _case_dir("crash_resume", side)
    out, cfg = work / "out", work / "cfg.yaml"
    cfg.write_text(SCENARIOS["chunk_checkpoint"][0].format(pg=PG, out=out, table=TABLE))
    # 1) crash after the 3rd chunk file lands
    crash = sh([str(binary), "run", "-c", str(cfg)],
               env=dict(CHILD_ENV, RIVET_TEST_PANIC_AT="after_chunk_file:3"), cwd=work)
    mid_files = len(list(out.rglob("*.parquet")))
    # 2) resume
    res = sh([str(binary), "run", "-c", str(cfg), "--resume"], env=CHILD_ENV, cwd=work)
    tasks = _chunk_task_states(work / ".rivet_state.db")
    # Raw parquet under a crashed prefix includes the un-manifested orphan the
    # crashed attempt left behind — counting it is the "artifacts are not
    # evidence" trap. Report BOTH: the manifest's delivered rows (the claim a
    # consumer reads) and the raw count (which must match on both binaries too,
    # since a changed orphan count is itself a behaviour change).
    delivered = None
    for m in sorted(out.rglob("manifest.json"))[:1]:
        j = json.loads(m.read_text())
        delivered = (j["status"], j["row_count"], j["part_count"])
    return {
        "crash_exit": crash.returncode,
        "files_after_crash": mid_files,
        "resume_exit": res.returncode,
        "manifest_delivered": delivered,
        "raw_parquet_rows": duckdb_rows(out),
        "chunk_task_states": tasks,
    }


def _chunk_task_states(state_db: Path) -> str:
    """The state DB's own account of the chunk tasks.

    Returns an `unreadable:` string rather than "" when the DB is missing or the
    table is not there: "" compares equal to "" and passed as `same`, so a
    binary that stopped writing chunk_task rows entirely — or a caller whose
    RIVET_STATE_URL sent the state to Postgres instead — graded nothing here and
    said so to nobody. (CHILD_ENV clears RIVET_STATE_URL for exactly that
    reason; this is the check that would notice if it ever stopped working.)
    """
    if not state_db.exists():
        return f"unreadable: no state DB at {state_db.name}"
    try:
        import sqlite3

        db = sqlite3.connect(state_db)
        return str(sorted((s, n) for s, n in db.execute(
            "SELECT status, count(*) FROM chunk_task GROUP BY status")))
    except Exception as e:  # noqa: BLE001
        return f"unreadable: {e}"


def failure_and_plan_cases(binary: Path, side: str) -> dict:
    """Two surfaces this work touched directly: the error path's exit code, and
    `plan --format json` stdout, which must be ONE parseable document (the pool
    advisory started leaking into it on this branch and was fixed)."""
    work = _case_dir("surfaces", side)
    base = dict(CHILD_ENV, RUST_LOG="error")

    blocker = work / "blocker"
    blocker.write_text("not a directory")
    bad = work / "bad.yaml"
    bad.write_text(SCENARIOS["full"][0].format(pg=PG, out=blocker / "sub", table=TABLE))
    fail = sh([str(binary), "run", "-c", str(bad)], env=base, cwd=work)

    multi = work / "multi.yaml"
    multi.write_text(f"""
source: {{ type: postgres, url: "{PG}" }}
exports:
  - name: ab_one
    table: {TABLE}
    mode: full
    format: parquet
    destination: {{ type: local, path: {work / 'o1'} }}
  - name: ab_two
    table: {TABLE}
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


# ── preflight ──────────────────────────────────────────────────────────────────
def _binary(label: str, raw: str, problems: list[str]) -> Path:
    p = Path(raw).expanduser().resolve()
    if not p.is_file():
        problems.append(f"{label} binary {p} is not a file")
        return p
    if not os.access(p, os.X_OK):
        problems.append(f"{label} binary {p} is not executable")
        return p
    v = run([str(p), "--version"], timeout=60)
    if not v.ok:
        problems.append(f"{label} binary {p} does not run: {(v.stderr or v.stdout).strip()[:120]}")
    else:
        print(f"   {label}: {p} ({v.stdout.strip()})")
    return p


def preflight(old_raw: str, new_raw: str) -> tuple[Path, Path]:
    """Everything this harness needs, checked BEFORE it seeds or runs anything.

    Each of these was a confusing failure: a missing `duckdb` was a bare
    FileNotFoundError three scenarios in; a Postgres container that was up but
    still recovering answered nothing and the seed silently wrote no rows; a
    mistyped binary path ran the whole matrix against `exit 127` on one side.
    """
    print("== preflight")
    problems: list[str] = []
    for tool, hint in (("docker", "the dev stand runs in docker"),
                       ("duckdb", "brew install duckdb — it is the INDEPENDENT readback")):
        if not have(tool):
            problems.append(f"`{tool}` is not on PATH ({hint})")
    old, new = _binary("old", old_raw, problems), _binary("new", new_raw, problems)
    if old == new:
        print("   ! old and new are the SAME binary — this is the harness self-test "
              "(expect zero differences), not a release comparison")

    if have("docker"):
        names = run(["docker", "ps", "--format", "{{.Names}}"], timeout=60).stdout.split()
        if PG_CONTAINER not in names:
            problems.append(
                f"container {PG_CONTAINER} is not running "
                f"(docker compose up -d postgres; or set RIVET_AB_PG_CONTAINER)")
        else:
            # Running is not ready: a Postgres still doing crash recovery accepts
            # `docker exec` and answers nothing.
            probe = run(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "rivet",
                         "-d", "rivet", "-tAc", "SELECT 1"], timeout=60)
            if probe.stdout.strip() != "1":
                problems.append(
                    f"{PG_CONTAINER} is up but not accepting queries yet "
                    f"({(probe.stderr or probe.stdout).strip().splitlines()[-1:] or ['no output']})")
            # The binaries connect over TCP to the URL, NOT through docker exec.
            # If those are two different servers, the seed lands where nothing
            # reads it — a whole matrix of empty exports with no explanation.
            m = re.search(r"@([^:/]+):(\d+)/", PG)
            if m and not tcp_open(m.group(1), int(m.group(2))):
                problems.append(f"nothing is listening on {m.group(1)}:{m.group(2)} "
                                f"— {PG} is not reachable the way rivet will reach it")
            elif m and f":{m.group(2)}" not in run(["docker", "port", PG_CONTAINER],
                                                   timeout=60).stdout:
                problems.append(
                    f"{PG_CONTAINER} does not publish port {m.group(2)} — the fixture would be "
                    f"seeded into a different server than {PG} points at")
    if problems:
        raise Fail("preflight failed — " + str(len(problems)) + " missing prerequisite(s):\n"
                   + "\n".join(f"      - {p}" for p in problems),
                   hint="nothing was seeded and no binary was run")
    print("   docker + duckdb present, "
          f"{PG_CONTAINER} accepting queries, both binaries answer --version")
    return old, new


def _usage() -> str:
    return ("usage: python3 -m dev.pytools.ab_regression <old-binary> <new-binary>\n"
            "  env: RIVET_AB_PG_URL / RIVET_AB_PG_CONTAINER / RIVET_AB_WORKDIR /\n"
            "       RIVET_AB_TAG (fixture namespace) / RIVET_AB_TIMEOUT")


def main() -> int:
    # Line-buffered, so this harness's stdout and `Fail`'s stderr interleave in
    # the order they were written when a CI job captures both into one log.
    sys.stdout.reconfigure(line_buffering=True)
    argv = sys.argv[1:]
    if any(a in ("-h", "--help") for a in argv):
        print(__doc__)
        print(_usage())
        return 0
    if len(argv) != 2:
        raise Fail("two binaries are required", hint=_usage())
    old, new = preflight(argv[0], argv[1])

    ROOT.mkdir(parents=True, exist_ok=True)
    seed()
    try:
        return compare(old, new)
    finally:
        # A namespaced fixture MUST be cleaned up or the dev database collects one
        # table per run forever. A teardown failure must not mask the verdict, so
        # it is reported rather than raised.
        drop = sh(["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", "rivet", "-d", "rivet",
                   "-tAc", f"DROP TABLE IF EXISTS {TABLE};"], timeout=600)
        if not drop.ok:
            print(f"   ! could not drop the fixture {TABLE}: "
                  f"{(drop.stderr or drop.stdout).strip()[:160]} — drop it by hand")


def compare(old: Path, new: Path) -> int:
    print(f"{'scenario':20} {'field':16} {'old':>22} {'new':>22}  verdict")
    print("=" * 96)
    regressions = []
    dead = []      # this cell compared nothing — a gate step that grades nothing FAILS
    partial = []   # this cell compared less than it claims to — loud, not fatal
    for name, (body, expect) in SCENARIOS.items():
        a = run_case(old, "old", name, body)
        b = run_case(new, "new", name, body)
        for field in ("exit", "rows_readback", "files"):
            same = a[field] == b[field]
            verdict = "same" if same else "DIFFERS"
            if not same:
                regressions.append((name, field, a[field], b[field]))
            print(f"{name:20} {field:16} {str(a[field]):>22} {str(b[field]):>22}  {verdict}")
        ma, mb = a["manifest"], b["manifest"]
        mdiff = manifest_diff(ma, mb)
        if mdiff:
            for key, va, vb in mdiff:
                regressions.append((name, f"manifest.{key}", va, vb))
                print(f"{name:20} {('manifest.' + key)[:16]:16} {va[:22]:>22} {vb[:22]:>22}  DIFFERS")
        else:
            shape = f"rows={ma['row_count']} parts={ma['part_count']}" if ma else "none"
            print(f"{name:20} {'manifest':16} {shape:>22} {shape:>22}  same")
        if expect is not None and b["rows_readback"] != expect:
            regressions.append((name, "expected_rows", expect, b["rows_readback"]))

        # ── did this cell grade anything at all? ──
        if a["files"] == 0 and b["files"] == 0:
            dead.append(f"{name}: NEITHER binary produced an output file "
                        f"(exit old={a['exit']} new={b['exit']}) — two empty runs compared equal. "
                        f"old stderr: {a['stderr_tail']}")
        if a["rows_readback"] == -1 and b["rows_readback"] == -1:
            partial.append(f"{name}: DuckDB could not read either side's parquet — "
                           "the independent readback graded nothing")
        if ma is None and mb is None and (a["files"] or b["files"]):
            partial.append(f"{name}: no manifest.json on either side — accounting and content "
                           "fingerprints were NOT compared")
        for side, mf in (("old", ma), ("new", mb)):
            if isinstance(mf, dict) and "parse_error" in mf:
                partial.append(f"{name}: {side} manifest did not parse ({mf['parse_error']}) — "
                               "fingerprints not compared")
        for side, res in (("old", a), ("new", b)):
            if res["manifest_files"] > 1:
                partial.append(f"{name}: {side} wrote {res['manifest_files']} manifests; only the "
                               "first was compared")

    for label, fn in (("crash_resume", crash_resume_case), ("surfaces", failure_and_plan_cases)):
        a, b = fn(old, "old"), fn(new, "new")
        for k in a:
            same = a[k] == b[k]
            if not same:
                regressions.append((label, k, a[k], b[k]))
            print(f"{label:20} {k:16} {str(a[k])[:22]:>22} {str(b[k])[:22]:>22}  {'same' if same else 'DIFFERS'}")
        if label == "crash_resume":
            # `sheds == 0 is also true of a governor that never ran` — same shape:
            # a resume comparison where NEITHER run crashed compared two clean
            # runs and called it a crash-recovery check.
            if a["crash_exit"] == 0 and b["crash_exit"] == 0:
                dead.append("crash_resume: the injected crash (RIVET_TEST_PANIC_AT="
                            "after_chunk_file:3) did not fire on EITHER binary — both runs "
                            "completed cleanly, so nothing about crash recovery was graded")
            if str(a["chunk_task_states"]).startswith("unreadable") and \
               str(b["chunk_task_states"]).startswith("unreadable"):
                partial.append(f"crash_resume: chunk_task states unreadable on both sides "
                               f"({a['chunk_task_states']}) — the state DB's own account of the "
                               "run was not compared")
        if label == "surfaces":
            if a["fail_exit"] == 0 and b["fail_exit"] == 0:
                dead.append("surfaces: the deliberately-broken config exited 0 on BOTH binaries — "
                            "the error path was never exercised, so its exit code was not graded")
            if str(a["plan_stdout"]).startswith("UNPARSEABLE") and \
               str(b["plan_stdout"]).startswith("UNPARSEABLE"):
                partial.append(f"surfaces: `plan --format json` was unparseable on both sides "
                               f"({a['plan_stdout']}) — the plan surface was not graded")
    print("=" * 96)

    if partial:
        print(f"\n{len(partial)} CELL(S) GRADED LESS THAN THEY CLAIM (not fatal, but read them):")
        for p in partial:
            print("  !", p)
    if dead:
        print(f"\n{len(dead)} CELL(S) GRADED NOTHING — a gate step that grades nothing must FAIL, "
              "not pass quietly:")
        for d in dead:
            print("  ✗", d)
    if regressions:
        print(f"\n{len(regressions)} DIFFERENCE(S) — each needs a verdict:")
        for r in regressions:
            print(" ", r)
    if regressions or dead:
        return 1
    print("\nno observable difference across", len(SCENARIOS) + 2, "scenarios")
    return 0


if __name__ == "__main__":
    shell_main(main)
