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
# Each entry is (reason, max_body_line_coverage_pct). The percentage is the
# MEASURED ceiling: an adjudication is a claim about DEPTH ("offline runs reach
# only the early-exit arms"), and entry counts cannot support it. A critic
# measured the first version and found two entries false by their own wording —
# `execute_resolved_plan` runs 66% of its body offline and `prepare_load` 70%,
# both reaching the closing brace and the return construction. Those two are
# gone: one now carries the reason its mutants.toml entry actually gives
# (credential-gated oracle), the other is listed as a candidate for
# UN-excluding rather than excused. Exceed the ceiling and the tool goes red —
# the adjudication is graded, not asserted.
ADJUDICATED: dict[str, tuple[str, int]] = {
    # Excluded as OUTPUT-IDENTICAL (a log line — `-> ()` changes no value, so
    # the lib suite cannot kill it by construction), NOT as unreachable; its
    # oracle is live (roast_mysql_cdc_warns_on_a_minimal_backlog...). 100% of
    # the body runs offline and that is the POINT, hence the ceiling of 100.
    "warn_positional_once": ("log-only fn, excluded as output-identical; live-oracled", 100),
    # Binary-dispatch paths. Offline cli tests SPAWN the rivet binary, so the
    # dispatcher and its arms record executions — but MEASURED body coverage is
    # 8-23%: the early-exit arms (help/arg validation/config errors), not the
    # live bodies. Ceilings are the measured value + headroom, so a future
    # offline test that reaches deeper turns this red instead of coasting.
    "dispatch": ("cli tests spawn the binary; measured 13% of body (early-exit arms)", 30),
    "run_plan_command": ("cli tests spawn the binary; measured 9% of body", 25),
    "check": ("cli tests spawn the binary; measured 8% of body (preflight/mod.rs)", 25),
    "introspect_all": ("reached via the binary's config-error arm; measured 23%", 35),
    "run_with_reconnect": ("reached via the binary's config-error arm; measured 23%", 35),
    # NOT an unreachability claim: the load subcommand's oracle is
    # CREDENTIAL-GATED (the warehouse live cells need BigQuery/Snowflake
    # credentials no offline run has), which is the reason .cargo/mutants.toml
    # gives at the entry itself. Body coverage is high (70%) because the cli
    # tests do drive the local path — so the ceiling is high on purpose and the
    # entry stands on the ORACLE being absent, not the code being unreached.
    "prepare_load": ("oracle is credential-gated (mutants.toml's own reason), not unreached", 85),
    # execute_resolved_plan is DELIBERATELY ABSENT: 66% of its body runs
    # offline, so its whole-function exclusion hides gradable mutants. It is
    # reported as CONTRADICTED until someone either un-excludes it or extracts
    # the decisions (five predicates already came out of it by hand — see
    # CLAUDE.md's live-only purity rule).
}


def live_only_functions() -> list[tuple[str, str]]:
    """Mirror tests/offline/live_only_purity_gate.rs: every `replace <fn>`
    whole-function entry in .cargo/mutants.toml, as (name, return_type).

    The RETURN TYPE is the entry's own discriminator and the only structural
    way to tell which `fn check` is meant: `replace check -> Result<bool>` is
    preflight::check, while preflight::cdc_health::check returns DoctorCheck.
    Without it the tool picked by coverage and credited the exclusion with 52
    executions of the unrelated namesake (measured 2026-08-29)."""
    text = (ROOT / ".cargo/mutants.toml").read_text()
    out: dict[str, str] = {}
    for m in re.finditer(r'"replace ([A-Za-z0-9_]+)( ->\s*([^"]*?))?(?: with [^"]*)?"', text):
        name, ret = m.group(1), (m.group(3) or "").strip()
        # Unescape the regex-literal spelling the config uses (`Result<\(\)>`).
        ret = ret.replace("\\", "")
        out[name] = ret
    return sorted(out.items())


def defining_files(name: str) -> set[str]:
    """src/ files holding `fn <name>(` — grep is the same authority the purity
    gate uses to resolve bodies."""
    try:
        # `fn name(` MISSES A GENERIC (`fn execute_load<R>(`) — and a miss made
        # `defining_files` empty, which made every file fail the scope filter,
        # which reported the exclusion as `no-symbol` instead of grading it. A
        # critic RED-proved that adding one type parameter to a signature turns
        # a real CONTRADICTED into a silent line of report (2026-08-29).
        got = subprocess.run(
            ["grep", "-rlE", rf"fn {name}\s*[(<]", str(ROOT / "src")],
            capture_output=True, text=True, check=False,
        ).stdout
    except OSError:
        return set()
    return {line.strip() for line in got.splitlines() if line.strip()}


def parse_lcov(lcov_path: Path) -> tuple[
    dict[str, list[tuple[str, int]]], dict[str, dict[int, int]]
]:
    """(file -> [(symbol, count)], file -> {line: hits}) from the lcov sections."""
    fns: dict[str, list[tuple[str, int]]] = {}
    lines: dict[str, dict[int, int]] = {}
    current = ""
    for line in lcov_path.read_text().splitlines():
        if line.startswith("SF:"):
            current = line[3:]
        elif line.startswith("FNDA:") and current:
            count, sym = line[5:].split(",", 1)
            fns.setdefault(current, []).append((sym, int(count)))
        elif line.startswith("DA:") and current:
            ln, hits = line[3:].split(",")[:2]
            lines.setdefault(current, {})[int(ln)] = int(hits)
    return fns, lines


def fn_signature(path: Path, name: str) -> str:
    """The text from `fn <name>` up to its opening brace — where the return
    type lives, however many lines the parameter list spans."""
    try:
        src = path.read_text()
    except OSError:
        return ""
    m = re.search(rf"\bfn {re.escape(name)}\s*[(<]", src)
    if not m:
        return ""
    brace = src.find("{", m.start())
    return src[m.start(): brace if brace != -1 else m.start()]


def body_span(path: Path, name: str) -> tuple[int, int] | None:
    """(first, last) 1-indexed line of `fn name`'s body, by brace counting."""
    try:
        src = path.read_text().splitlines()
    except OSError:
        return None
    for i, line in enumerate(src):
        if re.search(rf"\bfn {re.escape(name)}\s*[(<]", line):
            depth, started = 0, False
            for j in range(i, len(src)):
                depth += src[j].count("{") - src[j].count("}")
                if "{" in src[j]:
                    started = True
                if started and depth <= 0:
                    return (i + 1, j + 1)
            return None
    return None


def body_coverage_pct(lines: dict[int, int], span: tuple[int, int]) -> int:
    """Percent of INSTRUMENTED body lines that executed. The entry COUNT says
    nothing about depth — a critic measured two adjudications false by exactly
    this distinction (2 entries, 70% of the body, reaching the return)."""
    lo, hi = span
    inst = [h for ln, h in lines.items() if lo <= ln <= hi]
    if not inst:
        return 0
    return round(100 * sum(1 for h in inst if h > 0) / len(inst))


def main(argv: list[str]) -> int:
    lcov = Path(argv[1]) if len(argv) > 1 else ROOT / "target/llvm-cov/offline.lcov"
    if not lcov.is_file():
        print(
            f"live-only-cov: no lcov at {lcov} — run "
            f"`cargo llvm-cov nextest --lcov --output-path {lcov}` first"
        )
        return 2
    per_file, per_file_lines = parse_lcov(lcov)
    contradicted: list[str] = []
    adjudicated: list[str] = []
    confirmed: list[str] = []
    unresolved: list[str] = []
    for name, want_ret in live_only_functions():
        files = defining_files(name)
        if want_ret and len(files) > 1:
            # Keep only the definitions whose signature carries the entry's
            # return type — the structural answer to "which namesake".
            narrowed = {f for f in files if want_ret in fn_signature(Path(f), name)}
            if narrowed:
                files = narrowed
        if not files:
            # NOT a quiet report line: an exclusion whose function cannot be
            # found in src/ is either a dead mutants.toml entry or a signature
            # this tool cannot parse — both are ungradable CLAIMS, and letting
            # them pass is the absence-is-not-success defect inside the tool
            # that exists to enforce it.
            unresolved.append(name)
            continue
        # ONE file, not a set: `check` is defined in BOTH preflight/mod.rs (the
        # excluded one) and preflight/cdc_health.rs (unrelated), and summing
        # the set credited the exclusion with 52 foreign executions — the same
        # collision the file-scoping was introduced to remove, merely moved.
        # Prefer the file whose module path the v0 symbol names.
        best: tuple[int, str, int] | None = None
        for df in sorted(files):
            rel = df.removeprefix(str(ROOT) + "/")
            module = Path(rel).with_suffix("").name
            # The v0 symbol carries the WHOLE module path, so match on that
            # rather than on the bare ident: `preflight::check` (excluded) and
            # `preflight::cdc_health::check` (unrelated) share an ident, and
            # picking by file-set membership credited the exclusion with 52
            # foreign executions. Expected chain for src/preflight/mod.rs is
            # `9preflight5check`; for .../cdc_health.rs it is
            # `9preflight10cdc_health5check` — neither contains the other.
            mods = [m for m in Path(rel).with_suffix("").parts[1:] if m != "mod"]
            chain = "".join(f"{len(m)}{m}" for m in mods) + f"{len(name)}{name}"
            token = re.compile(re.escape(chain) + r"(?![a-z0-9_])")
            for f, records in per_file.items():
                if not (f.endswith(rel) or rel.endswith(f) or f == rel):
                    continue
                total = sum(c for sym, c in records if token.search(sym))
                span = body_span(Path(df), name)
                pct = body_coverage_pct(per_file_lines.get(f, {}), span) if span else 0
                cand = (total, f"{module}: {total} exec, {pct}% of body", pct)
                # The file the EXCLUSION means is the one whose module path the
                # symbol matches; when several do, the deepest coverage wins.
                if best is None or cand[0] > best[0]:
                    best = cand
        if best is None:
            unresolved.append(name)
            continue
        total, detail, pct = best
        if total == 0:
            confirmed.append(name)
        elif name in ADJUDICATED:
            reason, ceiling = ADJUDICATED[name]
            if pct > ceiling:
                contradicted.append(
                    f"{name}: {detail} — ADJUDICATION EXCEEDED (ceiling {ceiling}%): "
                    f"`{reason}` no longer describes what runs offline"
                )
            else:
                adjudicated.append(f"{name}({detail})")
        else:
            contradicted.append(f"{name}: {detail} — no adjudication")
    print(f"live-only-cov over {lcov}:")
    print(f"  confirmed zero-coverage : {len(confirmed)}")
    print(f"  adjudicated (see tool)  : {len(adjudicated)}"
          + (" — " + ", ".join(adjudicated) if adjudicated else ""))
    print(f"  UNRESOLVED (no fn in src): {len(unresolved)}"
          + (" — " + ", ".join(unresolved) if unresolved else ""))
    print(f"  CONTRADICTED            : {len(contradicted)}")
    for row in contradicted:
        print(f"    {row}")
    if unresolved:
        print("    an exclusion whose fn is not found in src/ is an ungradable "
              "claim: fix the signature match or drop the mutants.toml entry")
    return 1 if (contradicted or unresolved) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
