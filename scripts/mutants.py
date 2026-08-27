#!/usr/bin/env python3
"""Run cargo-mutants over one file and report the UNIQUE survivors.

Exists because the ad-hoc way of doing this failed silently three times in one
session, each time producing an empty survivor list that read as a perfect score:

  * `pgrep -f "cargo mutants"` never matches — the process is `cargo-mutants
    mutants ...`, with a hyphen — so the wait loop exits at once and the results
    are read before they are written;
  * `cargo mutants ... | tail -1` reports `tail`'s exit code, not the run's;
  * a killed run leaves its temp tree behind (three copies of `target/` at -j 3),
    and ~10 GB accumulated across a day of this.

So the completion condition is the LOG's own summary line, never a process check,
and the script REFUSES to print a verdict without it — an empty `missed.txt` from a
run that never started is indistinguishable from a clean file otherwise.
"""

import argparse
import pathlib
import re
import shutil
import subprocess
import sys
import time

DONE = re.compile(r"(\d+) mutants tested in .*?: (.*)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="source file to mutate, e.g. src/source/cdc/mod.rs")
    ap.add_argument("-j", "--jobs", type=int, default=3)
    ap.add_argument("--out", default=None, help="output dir (default: a temp one)")
    ap.add_argument("--timeout-min", type=int, default=180)
    args = ap.parse_args()

    out = pathlib.Path(args.out or f"/tmp/mutants-{pathlib.Path(args.file).stem}")
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    log = out / "run.log"

    with log.open("w") as fh:
        proc = subprocess.Popen(
            ["cargo", "mutants", "--file", args.file, "-j", str(args.jobs),
             "--output", str(out), "--", "--lib"],
            stdout=fh, stderr=subprocess.STDOUT,
        )
        deadline = time.time() + args.timeout_min * 60
        while proc.poll() is None and time.time() < deadline:
            time.sleep(15)
        if proc.poll() is None:
            proc.kill()

    text = log.read_text(errors="ignore")
    done = DONE.search(text)
    # The verdict is the SUMMARY LINE, not the exit code and not the file contents:
    # an aborted run leaves empty result files that look like a clean sweep.
    if not done:
        print(f"*** RUN DID NOT COMPLETE — no verdict. tail of {log}:", file=sys.stderr)
        print("\n".join(text.splitlines()[-5:]), file=sys.stderr)
        return 2

    print(f"{args.file}: {done.group(0)}")
    for kind in ("missed", "timeout"):
        f = out / "mutants.out" / f"{kind}.txt"
        rows = sorted(set(f.read_text().splitlines())) if f.is_file() else []
        print(f"\n--- {kind.upper()} ({len(rows)} unique) ---")
        for r in rows:
            print("  " + r.replace(args.file + ":", ""))

    for tmp in pathlib.Path("/private/var/folders").glob("*/*/T/cargo-mutants-*.tmp"):
        shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
