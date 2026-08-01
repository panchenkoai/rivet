#!/usr/bin/env python3
"""Execute `docs/scenario-artifact-matrix.yaml`: run each lifecycle scenario
against the CANONICAL GOLDEN SEED, then snapshot every artifact class and compare
it to the ledger's `expect` block.

    python3 -m dev.pytools.scenario_artifacts            # every scenario
    python3 -m dev.pytools.scenario_artifacts clean_full # one scenario
    python3 -m dev.pytools.scenario_artifacts --engine postgres

Why this exists, stated once so a later reader does not have to reconstruct it:
the release oracle proves DATA ARRIVES and the runner matrix proves a FEATURE
reached every runner, but neither asks what rivet LEFT BEHIND. Four defects found
on 2026-08-01 lived exactly there — a `run_status` row stuck `running` after a
resume, a `Failed` manifest that blocked every later load, an in-flight run
recorded as consumed, durable parts left uncounted — and every one was invisible
to a data check because the data was fine.

Two rules this harness holds itself to, both learned the same day:

* the source is the GOLDEN SEED (`seeds/common/<engine>.sql`, the fixture the
  oracle already uses), never a table the harness invents. Ad-hoc fixtures are
  why the dev stand drifted twice in one session and why none of those proofs is
  reproducible tomorrow;
* every scenario snapshots EVERY artifact class, not the one it was written
  about. Three of the four defects above hid while their own subsystem's tests
  passed, because each test looked only where its author was already looking.

A missing prerequisite is a SKIP with a reason, never a silent pass — the
distinction the oracle enforces and the one that makes a green run mean
something.

The YAML is parsed by hand (PyYAML is not a dependency of this repo, the same
constraint the release oracle works under). The parser is deliberately strict
about the shapes this one file uses and raises on anything else, so a malformed
ledger fails loudly instead of silently yielding zero scenarios.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from . import shell

REPO_ROOT = Path(__file__).resolve().parents[2]
MATRIX = REPO_ROOT / "docs" / "scenario-artifact-matrix.yaml"

# Golden-seed tables, per engine. These come from `seeds/common/<engine>.sql` —
# the harness READS them and never writes, so a scenario cannot corrupt the
# fixture the oracle also depends on.
GOLDEN = {
    "postgres": {
        "url": "postgres://rivet:rivet@127.0.0.1:5432/rivet",
        "table": "orders",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mysql": {
        "url": "mysql://rivet:rivet@127.0.0.1:3306/rivet",
        "table": "orders",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mssql": {
        "url": "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet",
        "table": "dbo.orders",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mongo": {
        "url": "mongodb://127.0.0.1:27017/rivet",
        "table": "orders",
        "pk": "_id",
        "cursor": None,
    },
}


# ── ledger ───────────────────────────────────────────────────────────────────
@dataclass
class Scenario:
    id: str
    engines: list[str]
    expect: dict[str, str] = field(default_factory=dict)


def parse_matrix(text: str) -> tuple[list[str], list[str], list[Scenario]]:
    """(declared engines, artifact class ids, scenarios).

    Hand-rolled for the one shape this file uses. Raises rather than returning
    an empty result on anything unexpected: a parser that quietly yields no
    scenarios turns the whole harness into a green no-op.
    """
    m = re.search(r"^engines: \[([^\]]*)\]", text, re.M)
    if not m:
        raise SystemExit(f"{MATRIX}: no top-level `engines: [...]`")
    declared = [e.strip() for e in m.group(1).split(",") if e.strip()]

    ac_start = text.index("\nartifact_classes:")
    sc_start = text.index("\nscenarios:")
    classes = re.findall(r"^  - id: (\S+)", text[ac_start:sc_start], re.M)
    if not classes:
        raise SystemExit(f"{MATRIX}: no artifact classes")

    scenarios: list[Scenario] = []
    cur: Scenario | None = None
    in_expect = False
    for line in text[sc_start:].splitlines():
        t = line.strip()
        if t.startswith("- id: "):
            if cur:
                scenarios.append(cur)
            cur = Scenario(id=t[len("- id: "):].strip(), engines=[])
            in_expect = False
            continue
        if cur is None:
            continue
        if t.startswith("engines: ["):
            cur.engines = [e.strip() for e in t[len("engines: ["):].rstrip("]").split(",") if e.strip()]
            in_expect = False
        elif t == "expect:":
            in_expect = True
        elif t.startswith(("what:", "note:")):
            in_expect = False
        elif in_expect and ":" in t and not t.startswith("#"):
            k, _, v = t.partition(":")
            # YAML quotes bounds like ">0"; strip them so the comparator sees
            # the operator, not a literal that would int()-crash mid-run.
            cur.expect[k.strip()] = v.strip().strip('"').strip("'")
    if cur:
        scenarios.append(cur)
    if not scenarios:
        raise SystemExit(f"{MATRIX}: no scenarios parsed")

    unknown = {k for s in scenarios for k in s.expect} - set(classes)
    if unknown:
        # The Rust guard pins this too; duplicated here so running the harness
        # against a hand-edited ledger cannot assert nothing.
        raise SystemExit(f"{MATRIX}: expect keys are not artifact classes: {sorted(unknown)}")
    return declared, classes, scenarios


# ── snapshot ─────────────────────────────────────────────────────────────────
def _sqlite_counts(db: Path) -> dict[str, int]:
    """Meta-DB artifact classes. A MISSING table is 0, not a crash: a scenario
    that never reaches the loader legitimately has no `load_run` table yet."""
    out = {
        "run_status_running": 0,
        "run_status_terminal": 0,
        "export_metrics_rows": 0,
        "files_committed": 0,
        "chunk_run_rows": 0,
        "chunk_task_running": 0,
        "file_log_rows": 0,
        "load_run_rows": 0,
        "loaded_source_run_ids": 0,
    }
    if not db.exists():
        return out
    con = sqlite3.connect(str(db))
    try:

        def one(sql: str) -> int:
            try:
                r = con.execute(sql).fetchone()
                return int(r[0]) if r and r[0] is not None else 0
            except sqlite3.Error:
                return 0

        out["run_status_running"] = one("SELECT COUNT(*) FROM run_status WHERE status='running'")
        out["run_status_terminal"] = one("SELECT COUNT(*) FROM run_status WHERE status<>'running'")
        out["export_metrics_rows"] = one("SELECT COUNT(*) FROM export_metrics")
        out["files_committed"] = one(
            "SELECT files_committed FROM export_metrics ORDER BY id DESC LIMIT 1"
        )
        out["chunk_run_rows"] = one("SELECT COUNT(*) FROM chunk_run")
        out["chunk_task_running"] = one("SELECT COUNT(*) FROM chunk_task WHERE status='running'")
        out["file_log_rows"] = one("SELECT COUNT(*) FROM file_log")
        out["load_run_rows"] = one("SELECT COUNT(*) FROM load_run")
        out["loaded_source_run_ids"] = one("SELECT COUNT(*) FROM loaded_source_run")
    finally:
        con.close()
    return out


def _prefix_counts(prefix: Path) -> dict[str, int]:
    """Destination artifact classes. Manifests are classified by their RECORDED
    status, not by filename: a `Failed` manifest is indistinguishable by name and
    is exactly the one that used to block every later load."""
    out = {
        "parts_on_disk": 0,
        "manifests_success": 0,
        "manifests_non_success": 0,
        "success_marker": 0,
        "canonical_manifest": 0,
    }
    if not prefix.exists():
        return out
    for p in prefix.rglob("*"):
        if not p.is_file():
            continue
        n = p.name
        if n.endswith(".parquet"):
            out["parts_on_disk"] += 1
        elif n == "_SUCCESS":
            out["success_marker"] += 1
        elif n == "manifest.json":
            out["canonical_manifest"] += 1
        elif n.startswith("manifest-") and n.endswith(".json"):
            try:
                st = json.loads(p.read_text()).get("status", "")
            except (OSError, json.JSONDecodeError):
                st = "<unreadable>"
            if str(st).lower() == "success":
                out["manifests_success"] += 1
            else:
                out["manifests_non_success"] += 1
    return out


def snapshot(work: Path, prefix: Path) -> dict[str, int]:
    snap = _sqlite_counts(work / ".rivet_state.db")
    snap.update(_prefix_counts(prefix))
    snap["checkpoint_file"] = 1 if (work / "cdc.ckpt").exists() else 0
    return snap


def compare(expect: dict[str, str], snap: dict[str, int]) -> list[str]:
    """Mismatches as human sentences. `[]` means MUST BE EMPTY — the assertion
    most of this ledger's rows use, because every defect it came from is
    'something survived that should not have'."""
    bad = []
    for key, want in expect.items():
        got = snap.get(key)
        if got is None:
            bad.append(f"{key}: not collected by the snapshot")
        elif want == "[]":
            if got != 0:
                bad.append(f"{key}: must be EMPTY, found {got}")
        elif want.startswith(">="):
            if got < int(want[2:]):
                bad.append(f"{key}: want >= {want[2:]}, got {got}")
        elif want.startswith(">"):
            if got <= int(want[1:]):
                bad.append(f"{key}: want > {want[1:]}, got {got}")
        else:
            if got != int(want):
                bad.append(f"{key}: want {want}, got {got}")
    return bad


# ── scenario execution ───────────────────────────────────────────────────────
def rivet_bin() -> str:
    return os.environ.get("RIVET", str(REPO_ROOT / "target" / "debug" / "rivet"))


def _cfg(work: Path, eng: str, name: str, mode_lines: str, top: str = "") -> Path:
    g = GOLDEN[eng]
    p = work / "rivet.yaml"
    p.write_text(
        f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
        f"exports:\n  - name: {name}\n    table: {g['table']}\n"
        f"{mode_lines}"
        f"    destination: {{ type: local, path: {work / 'out'} }}\n{top}"
    )
    return p


def _run(cfg: Path, eng: str, extra: Sequence[str] = (), env: dict | None = None):
    e = {"RIVET_SCEN_URL": GOLDEN[eng]["url"]}
    e.update(env or {})
    return shell.run([rivet_bin(), "run", "-c", str(cfg), *extra], env=e, timeout=900)


def engine_reachable(eng: str) -> bool:
    import socket

    port = {"postgres": 5432, "mysql": 3306, "mssql": 1433, "mongo": 27017}[eng]
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            return True
    except OSError:
        return False


# A run failure whose text names a MISSING COLUMN/TABLE is a drifted stand, not
# a rivet defect. `rivet check` does not probe cursor-column existence, so the
# drift only surfaces when the run issues its query — classify it there rather
# than reporting a red cell for someone else's fixture.
_DRIFT_SIGNATURES = (
    "invalid column name",
    "unknown column",
    "does not exist",
    "no such column",
    "invalid object name",
)


def looks_like_stand_drift(text: str) -> bool:
    t = text.lower()
    return any(sig in t for sig in _DRIFT_SIGNATURES)


def stand_matches_golden(eng: str, work: Path) -> tuple[bool, str]:
    """Does the running stand actually hold the GOLDEN SEED's shape?

    Checking is the point of seeding from a golden fixture: a harness that
    merely NAMES the seed still runs against whatever the stand happens to hold.
    That drift is not hypothetical — `dbo.orders` on this stand was replaced with
    a different fixture during unrelated work the same day, and without this
    check the scenario reported a confusing rivet error instead of "your stand is
    not the seed".

    Driven through `rivet check`, so the probe uses the product's own connector
    rather than a second per-engine client this file would have to maintain.
    A drifted stand is a SKIP with the reason — never a FAIL (it is not a rivet
    defect) and never a silent pass.
    """
    g = GOLDEN[eng]
    probe = work / "probe.yaml"
    cursor = g["cursor"]
    cols = f"    cursor_column: {cursor}\n" if cursor else ""
    probe.write_text(
        f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
        f"exports:\n  - name: probe\n    table: {g['table']}\n"
        f"    mode: {'incremental' if cursor else 'full'}\n{cols}"
        f"    format: parquet\n"
        f"    destination: {{ type: local, path: {work / 'probe_out'} }}\n"
    )
    r = shell.run(
        [rivet_bin(), "check", "-c", str(probe)],
        env={"RIVET_SCEN_URL": g["url"]},
        timeout=300,
    )
    if r.ok:
        return True, ""
    txt = (r.stdout + r.stderr).lower()
    if "invalid column" in txt or "does not exist" in txt or "unknown column" in txt:
        return False, (
            f"stand does not match seeds/common/{eng}.sql "
            f"({g['table']}: expected columns missing) — re-seed before trusting this row"
        )
    return False, f"{eng} preflight failed: {(r.stdout + r.stderr).strip()[:120]}"


def execute(sid: str, eng: str, work: Path) -> tuple[str, str]:
    """(status, detail): "ok" once the scenario's runs have been driven, or
    ("skip", why). Never returns ok without having run rivet."""
    g = GOLDEN[eng]
    name = f"scen_{sid}"
    if sid == "clean_full":
        cfg = _cfg(work, eng, name, "    mode: full\n    format: parquet\n")
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "clean_chunked":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "clean_chunked_checkpoint":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n"
            f"    chunk_checkpoint: true\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "crash_then_resume":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n"
            f"    chunk_checkpoint: true\n    format: parquet\n",
        )
        crash = _run(cfg, eng, env={"RIVET_TEST_PANIC_AT": "after_chunk_file:0"})
        if crash.ok:
            return ("fail", "the crash run unexpectedly succeeded — fixture is inert")
        r = _run(cfg, eng, extra=["--resume"])
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "keyset_parallel_clean":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
            f"    parallel: 4\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "keyset_parallel_worker_error":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
            f"    parallel: 4\n    format: parquet\n",
        )
        r = _run(cfg, eng, env={"RIVET_TEST_ERROR_AT": "keyset_parallel_worker:2"})
        if r.ok:
            return ("fail", "the injected worker error did not fail the run — fixture is inert")
        return ("ok", "")
    if sid == "incremental_twice":
        if not g["cursor"]:
            return ("skip", f"{eng} has no cursor column in the golden seed")
        cfg = _cfg(
            work, eng, name,
            f"    mode: incremental\n    cursor_column: {g['cursor']}\n    format: parquet\n",
        )
        a = _run(cfg, eng)
        b = _run(cfg, eng)
        return ("ok", "") if a.ok and b.ok else ("fail", (a.out + b.out).strip())
    # Everything else needs source WRITES (CDC) or warehouse credentials (load)
    # or a crafted prefix (gc) — those arrive with the next slice of the harness.
    # A named SKIP, never a silent pass: an unimplemented scenario must not read
    # as a green one.
    return ("skip", "scenario driver not implemented yet")


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    only_engine = None
    if "--engine" in args:
        i = args.index("--engine")
        only_engine = args[i + 1]
        del args[i:i + 2]
    only = args[0] if args and not args[0].startswith("-") else None

    declared, _classes, scenarios = parse_matrix(MATRIX.read_text())
    if not Path(rivet_bin()).exists():
        raise SystemExit(f"binary not found: {rivet_bin()} (cargo build --bin rivet)")

    print(f"scenario-artifacts: {len(scenarios)} scenario(s), engines {declared}")
    print(f"                    binary {rivet_bin()}")
    rows: list[tuple[str, str, str, str]] = []
    for sc in scenarios:
        if only and sc.id != only:
            continue
        for eng in sc.engines:
            if only_engine and eng != only_engine:
                continue
            if eng not in GOLDEN:
                rows.append((sc.id, eng, "SKIP", "no golden-seed mapping"))
                continue
            if not engine_reachable(eng):
                rows.append((sc.id, eng, "SKIP", f"{eng} not reachable"))
                continue
            work = Path(tempfile.mkdtemp(prefix=f"scen-{sc.id}-{eng}-"))
            try:
                ok, why = stand_matches_golden(eng, work)
                if not ok:
                    rows.append((sc.id, eng, "SKIP", why))
                    continue
                status, detail = execute(sc.id, eng, work)
                if status == "skip":
                    rows.append((sc.id, eng, "SKIP", detail))
                    continue
                if status == "fail":
                    if looks_like_stand_drift(detail):
                        rows.append((
                            sc.id, eng, "SKIP",
                            f"stand does not match seeds/common/{eng}.sql "
                            f"(a column the golden seed declares is missing) — re-seed",
                        ))
                    else:
                        # Truncate for the table only — the classifier above sees
                        # the whole thing, or a banner can hide the real error.
                        rows.append((sc.id, eng, "FAIL", f"run: {detail[:200]}"))
                    continue
                snap = snapshot(work, work / "out")
                bad = compare(sc.expect, snap)
                if bad:
                    rows.append((sc.id, eng, "FAIL", "; ".join(bad)))
                else:
                    rows.append((sc.id, eng, "PASS", ""))
            finally:
                shutil.rmtree(work, ignore_errors=True)

    width = max((len(r[0]) for r in rows), default=10)
    for sid, eng, st, detail in rows:
        mark = {"PASS": "✓", "FAIL": "✗", "SKIP": "⊘"}[st]
        print(f"  {mark} {sid:<{width}} {eng:<9} {detail}")
    failed = [r for r in rows if r[2] == "FAIL"]
    skipped = [r for r in rows if r[2] == "SKIP"]
    passed = [r for r in rows if r[2] == "PASS"]
    print(f"\nscenario-artifacts: {len(passed)} pass, {len(failed)} fail, {len(skipped)} skip")
    if failed:
        print("\nA mismatch is an ARTIFACT the scenario left in the wrong state — the")
        print("class every other gate is blind to. Fix the product, or correct the")
        print(f"expectation in {MATRIX.relative_to(REPO_ROOT)} with the reason.")
        return 1
    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
