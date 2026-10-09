"""Grade a live suite's self-skip log: libtest prints `ok` for a test that returned early, so
`skip_live` writes `RIVET-SKIP <module::fn> — <why>` to a file, and this reads it the way the gate
does. A skip is explained by `core.SKIP_ALLOWED` (the gate's list) or by a prerequisite the lane
declares it lacks (`--lacking NAME`, matched against the reason); any other skip is a test the lane
was to run and did not.

    python3 -m dev.release_oracle.skip_census target/rivet-skips.log --lacking BIGQUERY_TEST_PROJECT

`--verdicts LOG --lane ci|gate` grades the rig oracle's verdict log (`RIVET-ORACLE-<VERDICT> <test>
[<export>] — <detail>`, tests/common/verify.rs) instead: a passing test's stderr is hidden, so the
log is the only proof the oracle ran. A run that did not exit 0 logs REFUSED (graded against its
pre-run snapshot, tests/common/refusal.rs) or UNGRADED (the test crashed it); both are reported. Each count of runs the oracle did not fully grade must land
within its noise of the lane's ceiling, both ways: over it is a run the oracle used to grade and no
longer does; under it is a closed gap, and the ceiling comes down in the same PR.

    python3 -m dev.release_oracle.skip_census --verdicts target/rivet-oracle.log --lane ci
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
import tempfile
from pathlib import Path

from .core import SKIP_ALLOWED, self_skipped


def unexplained(skips: dict[str, str], lacking: list[str]) -> dict[str, str]:
    """The skips neither `SKIP_ALLOWED` nor a `lacking` prerequisite named in the reason explains."""
    return {t: why for t, why in skips.items()
            if t not in SKIP_ALLOWED and not any(l in why for l in lacking)}


#: A CDC stream's first run is SKIP when it delivered nothing and PARTIAL when it delivered some (timing decides), so one ceiling counts both.
FIRST_RUN = {"SKIP": "the stream's first run delivered nothing", "PARTIAL": "the stream's first graded run"}

#: Per lane, per counter: (ceiling, noise). Noise is what rivet's clock moves between runs of one commit (the
#: `cdc.backfill` SKIPs, `settle:` PARTIALs). The gate runs every live module (CI skips the warehouse, Mongo and
#: exclusive ones), so only the first-run counter transfers to it. Measured on #433 (CI run 37108478817):
#: first-run 61, all MySQL checkpoints a test wrote itself; with Rig::pin_binlog_here recording their anchor: 0.
#: `deferred`: the capped `rivet cdc` runs a lane runs (each owes its remainder to the stream's next run, graded there).
#: 2026-10-08 (#484, CI run 37720250883): skip 49 -> 53 and partial 39 -> 47 are the Oracle twins of cells already counted (the
#: `--pool --split` sibling a `--resume` run skips and its resumed plan, the two `settle:` cells); deferred 6 -> 8 the capped
#: `rivet cdc` drains on PostgreSQL and SQL Server.
#: 2026-10-09 (#508, CI run 37857783286 measured 66 = 42 `--resume` + 24 `settle:`): partial 47 -> 24. A `--resume` run is graded
#: against the source as it is now (it plans the keys the source holds now), so its 42 runs leave the counter: 26 counted on main,
#: 12 of the new crash-then-grow cells, 4 `run_resume_over_deleted_tasks_*` off `open_defect_`. The 24 left are `settle:` runs:
#: 20 counted on main and the 4 `incremental_adding_settle_*`, opted out (OFF) on main and graded now.
VERDICT_CEILINGS: dict[str, dict[str, tuple[int, int]]] = {
    "ci": {
        "first-run": (0,  # ratchet-pin: rig-oracle-first-run
                      0),
        "skip": (53,  # ratchet-pin: rig-oracle-skip
                 3),
        "partial": (24,  # ratchet-pin: rig-oracle-partial
                    3),
        "deferred": (8,  # ratchet-pin: rig-oracle-deferred
                     0),
    },
    "gate": {"first-run": (0,  # ratchet-pin: rig-oracle-first-run-gate
                           0)},
}


def verdict_counts(text: str) -> Counter:
    """Verdict lines per class, plus `first-run` and the SKIP/PARTIAL counts besides it (`skip`, `partial`)."""
    lines = [l for l in text.splitlines() if l.startswith("RIVET-ORACLE-")]
    n: Counter = Counter(re.match(r"RIVET-ORACLE-([A-Z]+) ", l).group(1) for l in lines if re.match(r"RIVET-ORACLE-[A-Z]+ ", l))
    first = {v: sum(1 for l in lines if l.startswith(f"RIVET-ORACLE-{v} ") and why in l) for v, why in FIRST_RUN.items()}
    n["first-run"] = sum(first.values())
    n["skip"], n["partial"] = n["SKIP"] - first["SKIP"], n["PARTIAL"] - first["PARTIAL"]
    n["deferred"] = n["DEFERRED"]
    return n


#: Reads repeated in one lane after the reader crashed in native code (tests/common/verify.rs); more means the reader is broken, not unlucky.
RERUN_CEILING = 3  # ratchet-pin: rig-oracle-rerun

#: The verdicts that grade a deferred run's remainder: the stream's next run compared with the source.
GRADED = ("PASS", "FAIL", "XFAIL", "PARTIAL")

#: What a known defect of a run that did not exit 0 says (tests/common/refusal.rs): it compares no source, so it pays no deferral.
REFUSAL_XFAIL = " — [a failed run left: "


def unpaid_deferrals(text: str) -> list[str]:
    """Every DEFERRED verdict no later graded verdict of the same test and export follows: a capped run whose remainder nothing graded."""
    out, owed = [], {}
    for l in text.splitlines():
        m = re.match(r"RIVET-ORACLE-([A-Z]+) (\S+) \[(.*?)\] — ", l)
        if not m:
            continue
        verdict, who = m.group(1), (m.group(2), m.group(3))
        if verdict == "DEFERRED":
            owed[who] = l
        elif verdict in GRADED and REFUSAL_XFAIL not in l:
            owed.pop(who, None)
    for (test, export), l in owed.items():
        out.append(f"{test} [{export}]: a capped run deferred its remainder and no later run of the stream was graded")
    return out


def verdict_reasons(text: str) -> Counter:
    """SKIP/PARTIAL/OFF lines per reason, numbers folded to N."""
    out: Counter = Counter()
    for l in text.splitlines():
        m = re.match(r"RIVET-ORACLE-(SKIP|PARTIAL|OFF|REFUSED|UNGRADED) [^—]*— (.*)", l)
        if m:
            why = re.sub(r" \{.*$", "", re.sub(r"grade(-load|-stdout)? \d+ ms: ?", "", m.group(2)))
            why = re.sub(r"`[^`]*`", "`X`", re.sub(r"=\S+;", "=X;", why))
            out[f"{m.group(1)} {re.sub(r'[0-9]+', 'N', why)}"] += 1
    return out


def verdict_errors(text: str, lane: str) -> list[str]:
    """What is wrong with a lane's verdict log: no verdict, no PASS, or a counter outside its band."""
    if not text.strip():
        return ["no rig oracle verdict was logged - the oracle did not run"]
    n = verdict_counts(text)
    if not n["PASS"]:
        return ["the rig oracle logged no PASS verdict"]
    errs = unpaid_deferrals(text)
    if n["RERUN"] > RERUN_CEILING:
        errs.append(f"{n['RERUN']} RERUN verdicts, ceiling {RERUN_CEILING}: the oracle's reader crashes too often to call it chance")
    for what, (ceiling, noise) in VERDICT_CEILINGS[lane].items():
        if n[what] > ceiling + noise:
            errs.append(f"{n[what]} {what} verdicts, ceiling {ceiling}: a run the oracle used to grade is no longer graded")
        elif n[what] < ceiling - noise:
            errs.append(f"{n[what]} {what} verdicts, ceiling {ceiling}: a gap closed - lower {lane}/{what} to {n[what]} "
                        "in VERDICT_CEILINGS so a later regression cannot spend the headroom")
    return errs


def verdict_report(text: str, lane: str) -> list[str]:
    """The counts a lane's census prints: per class, per reason, and each banded counter against its ceiling."""
    n = verdict_counts(text)
    out = [f"{v} {n[v]}" for v in ("PASS", "PARTIAL", "DEFERRED", "XFAIL", "SKIP", "OFF", "REFUSED", "UNGRADED", "RERUN", "FAIL")]
    out += ["per reason (numbers folded to N):"] + [f"{c:7d} {r}" for r, c in verdict_reasons(text).most_common()]
    out += [f"{what}: {n[what]} (ceiling {c} +-{z})" for what, (c, z) in VERDICT_CEILINGS[lane].items()]
    return out


def verify_oracle_verdict_census(led, log: Path | None = None) -> None:
    """Gate cell: the rig oracle graded the live modules this gate ran, and its ungraded first CDC runs stay within the gate's ceilings."""
    import os

    log = log or Path(os.environ.get("RIVET_ORACLE_LOG", ""))
    led.phase(f"Rig oracle verdict census · {log}")
    text = log.read_text() if log.is_file() else ""
    for line in verdict_report(text, "gate"):
        print(f"  {line}")
    errs = verdict_errors(text, "gate")
    for e in errs:
        led.failed("all", "-", "oracle-verdict-census", "-", f"rig oracle verdict census: {e} ({log})", "census")
    if not errs:
        n = verdict_counts(text)
        led.passed("all", "-", "oracle-verdict-census", "-",
                   f"rig oracle verdict census: first CDC runs ungraded {n['first-run']} (ceiling "
                   f"{VERDICT_CEILINGS['gate']['first-run'][0]}), PASS {n['PASS']}, SKIP {n['SKIP']}, "
                   f"PARTIAL {n['PARTIAL']}, OFF {n['OFF']}", "census")


def _lacking(raw: list[str]) -> list[str]:
    """`--lacking` values, each possibly several per line (a YAML block), blank lines dropped."""
    return [l.strip() for v in raw for l in v.splitlines() if l.strip()]


def _self_test() -> int:
    """RED-provable: drop the `t not in SKIP_ALLOWED` clause and the allowed record fails below."""
    allowed = next(iter(SKIP_ALLOWED))
    log = Path(tempfile.mkdtemp(prefix="rivet-skip-census-")) / "skips"
    log.write_text(f"RIVET-SKIP {allowed} — whatever it says\n"
                   "RIVET-SKIP live_x::needs_bq — x: BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset\n"
                   "RIVET-SKIP live_x::ran_nothing — pgbouncer-state (:6433) is down\n")
    skips = self_skipped(log)
    assert len(skips) == 3, skips
    assert unexplained(skips, ["BIGQUERY_TEST_PROJECT"]) == {
        "live_x::ran_nothing": "pgbouncer-state (:6433) is down"}, "the unexplained skip must be the ONE named by no list"
    assert set(unexplained(skips, [])) == {"live_x::needs_bq", "live_x::ran_nothing"}
    assert unexplained(skips, _lacking(["BIGQUERY_TEST_PROJECT\n\npgbouncer-state (:6433)\n"])) == {}
    assert unexplained(self_skipped(log.with_name("absent")), []) == {}, "no log is no skip"
    print("self-test ok: a self-skip is explained by SKIP_ALLOWED or a named lacking prerequisite, never silently")
    _verdict_self_test()
    return 0


def at_ceiling(lane: str) -> str:
    """A verdict log whose every banded counter of `lane` sits exactly at its ceiling."""
    c = {k: v[0] for k, v in VERDICT_CEILINGS[lane].items()}
    lines = ["RIVET-ORACLE-PASS live_x::a [e] — grade 12 ms: ok"]
    lines += [f"RIVET-ORACLE-PARTIAL live_x::f{i} [e] — grade 3 ms: {FIRST_RUN['PARTIAL']} (no source image)" for i in range(c["first-run"])]
    lines += [f"RIVET-ORACLE-SKIP live_x::s{i} [e] — stdout destination" for i in range(c.get("skip", 0))]
    lines += [f"RIVET-ORACLE-PARTIAL live_x::p{i} [e] — settle: rows past cursor_high" for i in range(c.get("partial", 0))]
    for i in range(c.get("deferred", 0)):
        lines += [f"RIVET-ORACLE-DEFERRED live_x::d{i} [t] — a bounded run reached --max-events 2",
                  f"RIVET-ORACLE-PASS live_x::d{i} [t] — grade 3 ms {{}}"]
    return "\n".join(lines) + "\n"


def _verdict_self_test() -> None:
    """RED-provable: one ungraded first CDC run more than the ceiling fails every lane; so does an empty log."""
    extra = f"RIVET-ORACLE-SKIP live_x::new [e] — grade 1 ms: {FIRST_RUN['SKIP']}, and no earlier source image\n"
    for lane in VERDICT_CEILINGS:
        log = at_ceiling(lane)
        assert verdict_errors(log, lane) == [], (lane, verdict_errors(log, lane))
        noise = VERDICT_CEILINGS[lane]["first-run"][1]
        errs = verdict_errors(log + extra * (noise + 1), lane)
        assert len(errs) == 1 and "first-run" in errs[0] and "no longer graded" in errs[0], (lane, errs)
        assert verdict_errors("", lane) and verdict_errors(log.replace("PASS", "OFF"), lane)
    rerun = "RIVET-ORACLE-RERUN live_x::a [e] — oracle reader crashed (grade): signal: 11 (SIGSEGV) after 1.0s; read once more\n"
    assert verdict_errors(at_ceiling("ci") + rerun * RERUN_CEILING, "ci") == [], "a rare reader crash is counted, not failed"
    assert any("RERUN" in e for e in verdict_errors(at_ceiling("ci") + rerun * (RERUN_CEILING + 1), "ci")), "a crashing reader fails the lane"
    n = verdict_counts(at_ceiling("ci") + "RIVET-ORACLE-SKIP t [e] — " + FIRST_RUN["SKIP"] + "\n")
    ci = {k: v[0] for k, v in VERDICT_CEILINGS["ci"].items()}
    assert (n["first-run"], n["skip"], n["partial"]) == (ci["first-run"] + 1, ci["skip"], ci["partial"]), n
    assert verdict_reasons("RIVET-ORACLE-OFF t [e] — grade-load 41 ms: why 7 {x: 1}") == Counter({"OFF why N": 1})
    assert verdict_reasons("RIVET-ORACLE-PARTIAL t [e] — grade-stdout 9 ms: why") == Counter({"PARTIAL why": 1})
    unpaid = at_ceiling("ci").replace("RIVET-ORACLE-PASS live_x::d0 [t]", "RIVET-ORACLE-SKIP live_x::d0 [t]")
    assert unpaid_deferrals(unpaid) == ["live_x::d0 [t]: a capped run deferred its remainder and no later run of the stream was graded"], \
        unpaid_deferrals(unpaid)
    assert any("deferred its remainder" in e for e in verdict_errors(unpaid, "ci")), "an unpaid deferral fails the lane"
    assert unpaid_deferrals("RIVET-ORACLE-DEFERRED a [t] — x\nRIVET-ORACLE-PASS a [u] — y\n"), "another export's verdict pays nothing"
    assert not unpaid_deferrals("RIVET-ORACLE-DEFERRED a [t] — x\nRIVET-ORACLE-PARTIAL a [t] — Mongo: only `_id`\n")
    for unpaying in ("REFUSED a [t] — exit 1: left nothing", "XFAIL a [t] — [a failed run left: observed-schema] known defect: y — z"):
        assert unpaid_deferrals(f"RIVET-ORACLE-DEFERRED a [t] — x\nRIVET-ORACLE-{unpaying}\n"), f"a failed run pays no deferral: {unpaying}"
    refused = ("RIVET-ORACLE-REFUSED t [e_1] — exit 3: left only failure-record x2; destination of `e_1` not compared: why\n"
               "RIVET-ORACLE-UNGRADED t [e] — exit 101: the test injected RIVET_TEST_PANIC_AT=after_part; a crash is graded by the run that resumes it\n")
    assert verdict_reasons(refused) == Counter({
        "REFUSED exit N: left only failure-record xN; destination of `X` not compared: why": 1,
        "UNGRADED exit N: the test injected RIVET_TEST_PANIC_AT=X; a crash is graded by the run that resumes it": 1}), verdict_reasons(refused)
    assert "REFUSED 1" in verdict_report(at_ceiling("ci") + refused, "ci") and "UNGRADED 1" in verdict_report(at_ceiling("ci") + refused, "ci")
    from .core import Ledger, Status

    log = Path(tempfile.mkdtemp(prefix="rivet-verdict-census-")) / "oracle.log"
    for body, want in ((at_ceiling("gate"), Status.PASS), (at_ceiling("gate") + extra, Status.FAIL)):
        log.write_text(body)
        led = Ledger(colour=False)
        verify_oracle_verdict_census(led, log)
        assert [c.status for c in led.cells] == [want], [(c.status, c.detail) for c in led.cells]
    print("self-test ok: the oracle verdict census fails one ungraded first CDC run past its ceiling, and an empty log")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("log", nargs="?", type=Path, help="the RIVET_SKIP_LOG file (default target/rivet-skips.log)")
    ap.add_argument("--lacking", action="append", default=[],
                    help="a prerequisite this lane does not provide; a skip whose reason names it is explained")
    ap.add_argument("--verdicts", type=Path, help="grade this rig oracle verdict log instead of a skip log")
    ap.add_argument("--lane", choices=sorted(VERDICT_CEILINGS), default="ci", help="whose ceilings --verdicts reads")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return _self_test()
    if ns.verdicts:
        text = ns.verdicts.read_text() if ns.verdicts.exists() else ""
        print("\n".join(verdict_report(text, ns.lane)))
        errs = verdict_errors(text, ns.lane)
        for e in errs:
            print(f"::error::{e} ({ns.verdicts})", file=sys.stderr)
        return 1 if errs else 0
    log = ns.log or Path("target/rivet-skips.log")
    skips = self_skipped(log)
    for t, why in sorted(skips.items()):
        print(f"SKIP {t} — {why}")
    bad = unexplained(skips, _lacking(ns.lacking))
    for t, why in sorted(bad.items()):
        print(f"::error::{t} skipped itself ({why}) and this lane was to run it: bring its prerequisite "
              "up, or name the one this lane lacks with --lacking", file=sys.stderr)
    print(f"self-skips: {len(skips)} ({log}), unexplained: {len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
