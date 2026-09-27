"""The nightly run: the full release gate, a prev-vs-cur soak, and (Sundays) the fault catalog.

The soak compares the SAME workload on both binaries: the current one must pass, and per
engine, mode and stream its median cycle time and peak RSS must stay within tolerance of
the previous release's. Reports land in dev/nightly_runs/<date>/. Scheduled by launchd
(dev/launchd/ai.rivet.nightly.plist.example); `--self-test` grades the comparison alone.

    uv run python dev/pytools/nightly.py [--soak 30m] [--skip-gate] [--faults]
"""

from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DUR_TOL, DUR_SLACK = 1.5, 0.5
RSS_TOL, RSS_SLACK = 1.3, 16.0


def soak_regressions(prev: dict, cur: dict) -> list[str]:
    """What the current soak did worse than the previous one (empty = within tolerance)."""
    out = [] if cur.get("verdict") == "PASS" else [f"current soak verdict {cur.get('verdict')}"]
    for eng, modes in cur.get("results", {}).items():
        for mode, res in modes.items():
            if not isinstance(res, dict) or "series" not in res:
                continue
            base = prev.get("results", {}).get(eng, {}).get(mode, {}).get("series", {})
            for st, s in res["series"].items():
                p = base.get(st)
                if not p or not s["dur_s"] or not p["dur_s"]:
                    continue
                cd, pd = statistics.median(s["dur_s"]), statistics.median(p["dur_s"])
                if cd > pd * DUR_TOL + DUR_SLACK:
                    out.append(f"{eng}/{mode}/{st}: median cycle {cd:.2f}s > {pd:.2f}s×{DUR_TOL}+{DUR_SLACK}")
                cr, pr = max(s["rss_mib"] or [0]), max(p["rss_mib"] or [0])
                if pr and cr > pr * RSS_TOL + RSS_SLACK:
                    out.append(f"{eng}/{mode}/{st}: peak RSS {cr:.0f}MiB > {pr:.0f}MiB×{RSS_TOL}+{RSS_SLACK:.0f}")
    return out


def soak(binary: Path, out: Path, duration: str) -> dict:
    """One soak run with `binary`, its report as a dict (empty on a harness failure)."""
    subprocess.run([sys.executable, "-m", "dev.pytools.soak", "--duration", duration,
                    "--bin", str(binary), "--out", str(out)], cwd=ROOT)
    rep = out / "soak-report.json"
    return json.loads(rep.read_text()) if rep.exists() else {}


def self_test() -> int:
    """A slower or fatter current soak is a regression; a failed one always is."""
    series = lambda d, r: {"results": {"pg": {"cdc": {"series": {"s": {"dur_s": d, "rss_mib": r}}}}},
                           "verdict": "PASS"}
    assert soak_regressions(series([1, 1, 1], [50]), series([1.2, 1.1, 1.3], [55])) == []
    assert soak_regressions(series([1, 1, 1], [50]), series([3, 3, 3], [50]))
    assert soak_regressions(series([1, 1, 1], [50]), series([1, 1, 1], [200]))
    failed = series([1], [50]) | {"verdict": "FAIL"}
    assert soak_regressions(series([1], [50]), failed)
    print("nightly self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    day = dt.date.today().isoformat()
    out = ROOT / "dev" / "nightly_runs" / day
    out.mkdir(parents=True, exist_ok=True)
    report = [f"# rivet nightly — {day}", ""]
    rc = 0
    if "--skip-gate" not in argv:
        g = subprocess.run(["make", "release-oracle-full"], cwd=ROOT,
                           stdout=open(out / "gate.log", "w"), stderr=subprocess.STDOUT)
        report.append(f"- gate: {'green' if g.returncode == 0 else 'RED'} (gate.log)")
        rc |= g.returncode != 0
    duration = argv[argv.index("--soak") + 1] if "--soak" in argv else "30m"
    prev = Path(os.environ.get("RIVET_PREV_RELEASE_BIN") or next(
        (ROOT / ".gate-baseline").glob("rivet-v*/rivet"), Path("missing")))
    cur = ROOT / "target" / "release" / "rivet"
    p, c = soak(prev, out / "soak-prev", duration), soak(cur, out / "soak-cur", duration)
    worse = soak_regressions(p, c) if p and c else ["a soak produced no report"]
    report.append(f"- soak prev vs cur ({duration} each): "
                  + ("within tolerance" if not worse else "REGRESSED: " + "; ".join(worse)))
    rc |= bool(worse)
    if "--faults" in argv or dt.date.today().weekday() == 6:
        f = subprocess.run([sys.executable, "dev/pytools/fault_catalog.py", "--out",
                            str(out / "faults.md")], cwd=ROOT)
        report.append(f"- fault catalog: {'every fault detected' if f.returncode == 0 else 'ESCAPES'} (faults.md)")
        rc |= f.returncode != 0
    (out / "report.md").write_text("\n".join(report) + "\n")
    print("\n".join(report))
    return 1 if rc else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
