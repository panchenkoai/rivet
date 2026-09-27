"""Apply each canonical product fault in dev/fault_catalog.yaml, run its detectors, record who caught it.

A fault is DETECTED only when at least one of its detector tests RAN and FAILED against it;
a clean pass is an escape, a build failure means the catalog entry is broken (not a verdict).
Every fault is applied from a snapshot of the working tree and restored from it (never
`git checkout`, which would drop uncommitted work), and the tree is verified afterwards.

    uv run python dev/pytools/fault_catalog.py [--only id,id] [--out report.md]
    python3 dev/pytools/fault_catalog.py --self-test
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CARGO = os.environ.get("CARGO", "cargo")
sys.path.insert(0, str(ROOT))


def load(path: Path) -> list[dict]:
    """The catalog's fault entries."""
    import yaml

    return yaml.safe_load(path.read_text()) or []


def detectors_command(suite: str, names: list[str]) -> list[str]:
    """The cargo invocation that runs exactly `names` in `suite`."""
    if suite == "lib":
        return [CARGO, "nextest", "run", "--lib", "--run-ignored", "all", "--no-fail-fast",
                "-E", " | ".join(f"test(/::{n}$/)" for n in names)]
    return [CARGO, "nextest", "run", "--test", suite, "--run-ignored", "all", "--no-fail-fast",
            "-E", " | ".join(f"test(/::{n}$/)" for n in names)]


def verdict(outcomes: dict[str, str], names: list[str], built: bool) -> str:
    """detected / escaped / unviable / not-run for one fault."""
    if not built:
        return "unviable"
    ran = {k.rsplit("::", 1)[-1]: v for k, v in outcomes.items()}
    seen = [n for n in names if n in ran]
    if not seen:
        return "not-run"
    return "detected" if any(ran[n].startswith("FAIL") for n in seen) else "escaped"


def run_fault(f: dict) -> tuple[str, str]:
    """Apply one fault, run its detectors, restore; (verdict, detail)."""
    from dev.release_oracle.core import nextest_outcomes

    path = ROOT / f["file"]
    snap = path.read_bytes()
    text = snap.decode()
    if text.count(f["old"]) != 1:
        return "unviable", f"anchor matches {text.count(f['old'])} times in {f['file']}"
    try:
        path.write_text(text.replace(f["old"], f["new"], 1))
        os.utime(path, None)
        by_suite: dict[str, list[str]] = {}
        for d in f["detectors"]:
            by_suite.setdefault(d["suite"], []).append(d["test"])
        outcomes: dict[str, str] = {}
        built = True
        tail = ""
        for suite, names in by_suite.items():
            p = subprocess.run(detectors_command(suite, names), cwd=ROOT, capture_output=True, text=True)
            out = p.stdout + p.stderr
            if "error[E" in out or "could not compile" in out:
                built = False
                tail = out[-400:]
            outcomes.update(nextest_outcomes(out))
        names = [d["test"] for d in f["detectors"]]
        v = verdict(outcomes, names, built)
        return v, tail or json.dumps({k.rsplit("::", 1)[-1]: v for k, v in outcomes.items()})
    finally:
        path.write_bytes(snap)
        os.utime(path, None)


def self_test() -> int:
    """The verdict rules: a run failure is detection, a clean run an escape, nothing run is not a verdict."""
    assert verdict({"live_suite::m::t": "FAIL"}, ["t"], True) == "detected"
    assert verdict({"live_suite::m::t": "PASS"}, ["t"], True) == "escaped"
    assert verdict({"live_suite::m::other": "FAIL"}, ["t"], True) == "not-run"
    assert verdict({}, ["t"], False) == "unviable"
    assert "--no-fail-fast" in detectors_command("live_suite", ["a"])
    print("fault_catalog self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    faults = load(ROOT / "dev" / "fault_catalog.yaml")
    if "--only" in argv:
        keep = set(argv[argv.index("--only") + 1].split(","))
        faults = [f for f in faults if f["id"] in keep]
    status = ["git", "status", "--porcelain", "--", "src"]
    before = subprocess.run(status, cwd=ROOT, capture_output=True, text=True).stdout
    rows = []
    for f in faults:
        v, detail = run_fault(f)
        rows.append((f["id"], f["class"], v, detail))
        print(f"{v:9} {f['id']}: {detail[:160]}", flush=True)
    after = subprocess.run(status, cwd=ROOT, capture_output=True, text=True).stdout
    if after != before:
        print(f"TREE NOT RESTORED — src/ differs from before the run:\n{after}")
        return 2
    report = ["| fault | class | verdict |", "|---|---|---|"]
    report += [f"| `{i}` | {c} | {v} |" for i, c, v, _ in rows]
    detected = sum(1 for r in rows if r[2] == "detected")
    report.append(f"\n{detected} of {len(rows)} detected.")
    if "--out" in argv:
        Path(argv[argv.index("--out") + 1]).write_text("\n".join(report) + "\n")
    print("\n".join(report))
    return 0 if detected == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
