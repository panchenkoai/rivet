"""Does a run's history change what the NEXT run does?

The sweeps prove that one run's numbers reconcile. This asks the other question:
whether the state backend, which accumulates every run ever made, can make a
later run behave differently from the same run on an empty database. A leak of
that kind is invisible in any single sweep — every case still agrees with itself
— and it is the failure mode a shared deployment lives with, because nobody's
production state DB is ever empty.

THREE PHASES, and the third is the one that can actually bite:

  A  clean       every state table truncated, every `.rivet_state.db` deleted
  B  prefilled   the same suites again over A's history, under DIFFERENT export
                 names — so a difference means UNRELATED history changed a run
  C  same names  the same suites again under A's OWN export names — a scheduled
                 export re-running over its own history

A vs B is the isolation question: rows about other exports must not reach this
one. A vs C is not a bug hunt in the same sense — resume is the POINT of keeping
state, so a difference there is expected and the check is that it is the
DOCUMENTED difference (a resumed run captures nothing new) rather than an
arbitrary one. Both are reported; only A-vs-B disagreement is a defect by itself.

The batch sweep has no per-invocation token — its export names are fixed — so it
contributes to A and C only, which is exactly the shape C is about.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
CDC_SWEEP = ROOT / "dev" / "pytools" / "cdc_sweep.py"
BATCH_SWEEP = ROOT / "dev" / "pytools" / "extraction_sweep.py"

#: Row of a sweep's result table: engine, case, then the numbers, then a verdict.
ROW = re.compile(r"^\s{2}(\S+)\s+(\S+)\s+(.*)$")


def sh(argv: list[str], env: dict | None = None, timeout: int = 5400):
    return subprocess.run(
        argv, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout
    )


def psql_state(sql: str) -> str:
    return sh(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    ).stdout.strip()


def wipe_state() -> str:
    """Truncate every table in the Postgres state DB, and delete every SQLite one.

    TRUNCATE rather than DROP DATABASE: the schema and its migration version stay
    exactly as a real deployment's, so this measures an EMPTY database and not a
    differently-shaped one. Dropping and recreating would also re-run migrations,
    which is a different thing to test and would hide a defect that only appears
    on an already-migrated schema.
    """
    tables = [
        t for t in psql_state(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'"
        ).splitlines() if t.strip()
    ]
    if tables:
        psql_state("TRUNCATE " + ", ".join(f'"{t}"' for t in tables) + " RESTART IDENTITY CASCADE")
    killed = []
    for base in (ROOT, Path("/tmp")):
        for p in base.rglob(".rivet_state.db*"):
            try:
                p.unlink()
                killed.append(str(p))
            except OSError:
                pass
    left = psql_state("SELECT count(*) FROM export_metrics") or "?"
    return f"{len(tables)} state table(s) truncated (export_metrics now {left}), " \
           f"{len(killed)} sqlite state file(s) removed"


def parse(out: str) -> dict[tuple[str, str], str]:
    """(engine, case) -> verdict, from a sweep's printed table.

    The VERDICT is what gets compared, not the row counts: two runs of a CDC case
    legitimately differ in how many events they capture (a resumed run captures
    none), and comparing counts would flag that as a leak while missing an actual
    behaviour change that kept the same numbers.
    """
    seen = {}
    for line in out.splitlines():
        m = ROW.match(line)
        if not m or line.strip().startswith(("engine", "case", "-", "axes")):
            continue
        engine, case, rest = m.groups()
        parts = rest.split()
        # A data row has NUMBERS between the name and the verdict. Without this
        # the two suites' header lines parse as a case called `src` whose verdict
        # is the rest of the header — a phantom row that would then "differ"
        # between phases and be reported as a leak.
        if not parts or not any(t.isdigit() or t == "-" for t in parts[:3]):
            continue
        # the verdict is everything after the numeric columns
        verdict = rest
        for i, tok in enumerate(parts):
            if not (tok.isdigit() or tok == "-"):
                verdict = " ".join(parts[i:])
                break
        kind = "OK" if verdict.startswith("OK") else "SKIP" if verdict.startswith("SKIP") else "FAIL"
        seen[(engine, case)] = kind if kind != "FAIL" else f"FAIL: {verdict[:90]}"
    return seen


def run_cdc(token: str, work: str) -> dict[tuple[str, str], str]:
    p = sh(
        [sys.executable, "-u", str(CDC_SWEEP)],
        env={"RIVET_CDC_SWEEP_TOKEN": token, "RIVET_CDC_SWEEP_WORK": work},
    )
    print(p.stdout[-400:] if p.returncode not in (0, 1) else "", end="")
    return parse(p.stdout)


def run_batch(work: str) -> dict[tuple[str, str], str]:
    p = sh([sys.executable, "-u", str(BATCH_SWEEP)], env={"RIVET_SWEEP_WORK": work})
    # The batch sweep prints one column fewer (no engine), so give every row the
    # engine it actually uses rather than letting the parser invent one.
    out = {}
    for (a, b), v in parse(p.stdout).items():
        out[("batch", a)] = v
        _ = b
    return out


def diff(
    label: str, base: dict[tuple[str, str], str], other: dict[tuple[str, str], str]
) -> list[str]:
    lines = []
    for key in sorted(set(base) | set(other)):
        x, y = base.get(key, "<absent>"), other.get(key, "<absent>")
        if x != y:
            lines.append(f"    {key[0]}/{key[1]}: clean={x}  {label}={y}")
    return lines


def main() -> int:
    work = os.environ.get("RIVET_ISO_WORK", "/tmp/rivet_iso")
    print("=== phase A: CLEAN state ===")
    print("  " + wipe_state())
    a_cdc = run_cdc("cleanA", f"{work}/a_cdc")
    a_batch = run_batch(f"{work}/a_batch")
    a = {**a_cdc, **a_batch}
    print(f"  {sum(1 for v in a.values() if v == 'OK')} OK / {len(a)} cases on a clean database")

    print("\n=== phase B: PREFILLED state, DIFFERENT export names ===")
    print(f"  export_metrics now holds {psql_state('SELECT count(*) FROM export_metrics')} row(s)")
    b = run_cdc("prefB", f"{work}/b_cdc")
    print(f"  {sum(1 for v in b.values() if v == 'OK')} OK / {len(b)} cases")

    print("\n=== phase C: PREFILLED state, THE SAME export names as phase A ===")
    c = {**run_cdc("cleanA", f"{work}/c_cdc"), **run_batch(f"{work}/c_batch")}
    print(f"  {sum(1 for v in c.values() if v == 'OK')} OK / {len(c)} cases")

    print("\n=== A vs B — unrelated history must not change a run ===")
    leaks = diff("prefilled", a_cdc, b)
    print("\n".join(leaks) if leaks else "    no difference")

    print("\n=== A vs C — the same export re-run over its own history ===")
    print("    (a difference here is resume doing its job; the question is whether it is")
    print("     the DOCUMENTED difference and not an arbitrary one)")
    repeats = diff("same-name", a, c)
    print("\n".join(repeats) if repeats else "    no difference")

    if leaks:
        print(
            f"\n  {len(leaks)} case(s) behave differently on a prefilled database under a "
            f"DIFFERENT name — that is state from unrelated runs reaching this one"
        )
        return 1
    print("\n  no unrelated-history leak")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
