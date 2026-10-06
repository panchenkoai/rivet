"""A stand-in `rivet` that replays one scenario from `scenario.json` beside it and logs every call."""

import json
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE_FLAGS = {
    "run": ["--config", "--export", "--summary-output", "--json", "--json-errors"],
    "plan": ["--config", "--export", "--format", "--output", "--json-errors"],
    "apply": ["--resume", "--force", "--json-errors"],
    "load": ["--config", "--run-id", "--pool", "--json-errors"],
    "compact": ["--config", "--run-id", "--pool", "--json-errors"],
    "metrics": ["--config", "--export", "--last", "--json", "--json-errors"],
}


def value_of(argv, flag):
    """The value following a flag, or None."""
    return argv[argv.index(flag) + 1] if flag in argv else None


def main():
    """Answer `--version` / `--help`, or replay the scenario for the subcommand."""
    spec = json.loads((HERE / "scenario.json").read_text())
    argv = sys.argv[1:]
    if argv == ["--version"]:
        print(f"rivet {spec.get('version', '0.31.0')} (fake)")
        return 0
    sub = argv[0]
    if "--help" in argv:
        flags = BASE_FLAGS.get(sub, []) + spec.get("flags", {}).get(sub, []) + spec.get("flags", {}).get("*", [])
        print("Usage: rivet " + sub + " [OPTIONS]\n\nOptions:\n" + "\n".join(f"      {f} <X>" for f in flags))
        return 0
    seen = {k: os.environ.get(k) for k in spec.get("record_env", [])}
    with open(HERE / "calls.jsonl", "a") as log:
        log.write(json.dumps({"argv": argv, "env": seen, "cwd": os.getcwd()}) + "\n")
    play = spec.get(sub, {})
    if sub == "plan" and not play.get("exit"):
        out = value_of(argv, "--output")
        expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
        if out:
            Path(out).write_text(json.dumps({"export_name": value_of(argv, "--export"), "expires_at": expires}))
        else:
            print(json.dumps(spec.get("plan_list", [])))
        return 0
    if sub == "metrics":
        print(json.dumps(play.get("rows", [])))
        return play.get("exit", 0)
    if play.get("stderr"):
        print(play["stderr"], file=sys.stderr)
    for name in spec.get("leak_env", []):
        print(f"connecting with {os.environ.get(name)}", file=sys.stderr)
    target = value_of(argv, "--summary-output")
    if target and play.get("summary") is not None:
        Path(target).write_text(json.dumps(play["summary"]))
    if play.get("line") is not None:
        print(json.dumps(play["line"]), file=sys.stderr)
    sys.stderr.flush()
    if play.get("signal"):
        os.kill(os.getpid(), getattr(signal, play["signal"]))
    return play.get("exit", 0)


sys.exit(main())
