"""Empirical check of the live-only claim: mutants.toml whole-function
exclusions vs MEASURED offline coverage.

`.cargo/mutants.toml` excludes whole functions, mostly as "live-only" (no
offline test reaches them — the purity gate holds those bodies to glue-only).
That reachability claim was never measured. This tool takes an offline-battery
.lcov (cargo llvm-cov nextest) and classifies every excluded function:

  CONTRADICTED — offline tests DO execute it IN ITS DEFINING FILE and no
                 adjudication explains why the exclusion still stands.
  adjudicated  — executed offline, with the reason pinned below (e.g. a
                 log-only fn excluded as output-identical, not as unreachable;
                 or a binary-dispatch path whose offline executions are the
                 early-exit arms only).
  confirmed    — zero executions: the exclusion is honest.
  no-symbol    — no FNDA record in the defining file: inlined away or renamed.
                 NOT proof of zero coverage (absence is not success).

Matching is FILE-SCOPED: lcov FN/FNDA records live under SF: sections, and the
v0 ident token (?<!digit)<len><name>(?![a-z0-9_]) is summed only inside files
that define `fn <name>(` — the bare-token first cut credited preflight's
excluded `check` with cdc_health.rs's unrelated `fn check` (the same collision
mutants.toml's own `$`-anchor comment documents).
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Exclusions that ARE executed offline, each with the reason the exclusion
# still stands. An entry here is a claim someone verified; delete it and the
# tool goes red on that fn, which is exactly the point.
ADJUDICATED: dict[str, str] = {
    # Excluded as OUTPUT-IDENTICAL (a log line — `-> ()` changes no value, so
    # the lib suite cannot kill it by construction), NOT as unreachable; its
    # oracle is live (roast_mysql_cdc_warns_on_a_minimal_backlog...). The
    # offline executions are the sink unit tests running past the log.
    "warn_positional_once": "log-only fn, excluded as output-identical; live-oracled",
    # Binary-dispatch paths: offline cli tests SPAWN the rivet binary, so the
    # dispatcher and its command arms record executions — but those runs
    # exercise the early-exit arms (help/arg validation/config errors), not
    # the live bodies the exclusions are about. The purity gate holds these
    # bodies to glue-only; their DECISIONS live in named predicates that ARE
    # offline-graded. Measured 2026-08-29: dispatch 32, run_plan_command 2,
    # execute_resolved_plan 3, prepare_load 2, check 2 (preflight/mod.rs),
    # introspect_all 1, run_with_reconnect 3.
    "dispatch": "cli tests spawn the binary; executions are early-exit arms",
    "run_plan_command": "cli tests spawn the binary; executions are early-exit arms",
    "execute_resolved_plan": "cli tests spawn the binary; executions are early-exit arms",
    "prepare_load": "cli tests spawn the binary; executions are early-exit arms",
    "check": "cli tests spawn the binary; executions are early-exit arms (preflight/mod.rs)",
    "introspect_all": "reached via the binary's config-error arm only",
    "run_with_reconnect": "reached via the binary's config-error arm only",
}


def live_only_functions() -> list[str]:
    """Mirror tests/offline/live_only_purity_gate.rs: every `replace <fn>`
    whole-function entry in .cargo/mutants.toml, by leading ident."""
    text = (ROOT / ".cargo/mutants.toml").read_text()
    out = set()
    for m in re.finditer(r'"replace ([A-Za-z0-9_]+)( ->|( |"))', text):
        name = m.group(1)
        tail = text[m.end(1) : m.end(1) + 24]
        if tail.startswith(" ->") or tail.startswith('"') or tail.startswith(" with"):
            out.add(name)
    return sorted(out)


def defining_files(name: str) -> set[str]:
    """src/ files holding `fn <name>(` — grep is the same authority the purity
    gate uses to resolve bodies."""
    try:
        got = subprocess.run(
            ["grep", "-rl", f"fn {name}(", str(ROOT / "src")],
            capture_output=True, text=True, check=False,
        ).stdout
    except OSError:
        return set()
    return {line.strip() for line in got.splitlines() if line.strip()}


def parse_lcov(lcov_path: Path) -> dict[str, list[tuple[str, int]]]:
    """file -> [(symbol, count)] from SF-sectioned FNDA records."""
    out: dict[str, list[tuple[str, int]]] = {}
    current = ""
    for line in lcov_path.read_text().splitlines():
        if line.startswith("SF:"):
            current = line[3:]
        elif line.startswith("FNDA:") and current:
            count, sym = line[5:].split(",", 1)
            out.setdefault(current, []).append((sym, int(count)))
    return out


def main(argv: list[str]) -> int:
    lcov = Path(argv[1]) if len(argv) > 1 else ROOT / "target/llvm-cov/offline.lcov"
    if not lcov.is_file():
        print(
            f"live-only-cov: no lcov at {lcov} — run "
            f"`cargo llvm-cov nextest --lcov --output-path {lcov}` first"
        )
        return 2
    per_file = parse_lcov(lcov)
    contradicted: list[tuple[str, int]] = []
    adjudicated: list[tuple[str, int]] = []
    confirmed: list[str] = []
    nosymbol: list[str] = []
    for name in live_only_functions():
        token = re.compile(rf"(?<![0-9]){len(name)}{re.escape(name)}(?![a-z0-9_])")
        files = defining_files(name)
        total = 0
        matched = 0
        for f, records in per_file.items():
            if not any(f.endswith(df.removeprefix(str(ROOT) + "/")) or df.endswith(f) or f == df for df in files):
                continue
            for sym, count in records:
                if token.search(sym):
                    matched += 1
                    total += count
        if matched == 0:
            nosymbol.append(name)
        elif total == 0:
            confirmed.append(name)
        elif name in ADJUDICATED:
            adjudicated.append((name, total))
        else:
            contradicted.append((name, total))
    print(f"live-only-cov over {lcov}:")
    print(f"  confirmed zero-coverage : {len(confirmed)}")
    print(f"  adjudicated (see tool)  : {len(adjudicated)}"
          + (" — " + ", ".join(f"{n}({c})" for n, c in adjudicated) if adjudicated else ""))
    print(f"  no-symbol (inlined?)    : {len(nosymbol)}"
          + (" — " + ", ".join(nosymbol) if nosymbol else ""))
    print(f"  CONTRADICTED            : {len(contradicted)}")
    for name, total in sorted(contradicted, key=lambda x: -x[1]):
        print(f"    {name}: {total} offline execution(s) in its defining file — "
              f"either un-exclude it (its mutants are gradable) or adjudicate "
              f"with the reason in dev/pytools/live_only_cov.py")
    return 1 if contradicted else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
