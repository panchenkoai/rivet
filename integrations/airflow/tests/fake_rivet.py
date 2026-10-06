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
QUERY_FILE_RULES = (
    "query_file must be a relative path",
    "query_file path must not contain '..'",
    "resolves outside the config directory",
)


def value_of(argv, flag):
    """The value following a flag, or None."""
    return argv[argv.index(flag) + 1] if flag in argv else None


def read_config(argv):
    """The parsed config a call names, and its directory."""
    cfg = value_of(argv, "--config")
    if not cfg:
        return {}, None
    import yaml

    return yaml.safe_load(Path(cfg).read_text()) or {}, Path(cfg).resolve().parent


def query_files(sub, argv, doc, base):
    """rivet's `query_file` rule (src/config/export.rs, src/config/mod.rs): the refusal text, and the SQL read."""
    only, read = value_of(argv, "--export"), {}
    for entry in doc.get("exports") or []:
        name, ref = entry.get("name"), entry.get("query_file")
        if not ref:
            continue
        if Path(ref).is_absolute():
            return f"export '{name}': {QUERY_FILE_RULES[0]}: '{ref}'", read
        if ".." in Path(ref).parts:
            return f"export '{name}': {QUERY_FILE_RULES[1]}: '{ref}'", read
        if sub not in ("run", "plan") or only not in (None, name):
            continue
        joined = base / ref
        if joined.exists() and base not in joined.resolve().parents:
            return f"export '{name}': query_file '{ref}' {QUERY_FILE_RULES[2]}", read
        if not joined.is_file():
            return "No such file or directory (os error 2)", read
        read[name] = joined.read_text()
    return None, read


def unit_of(sub, argv, doc):
    """The export and table a call is about, as far as its arguments say."""
    export = value_of(argv, "--export")
    if export is None and sub == "apply" and len(argv) > 1:
        try:
            export = json.loads(Path(argv[1]).read_text()).get("export_name")
        except (OSError, ValueError):
            export = None
    names = [e.get("name") for e in doc.get("exports") or []]
    if export is None and sub in ("load", "compact") and len(names) == 1:
        export = names[0]
    return export or "", value_of(argv, "--table") or ""


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
    doc, base = read_config(argv)
    refusal, sql = query_files(sub, argv, doc, base) if base else (None, {})
    export, table = unit_of(sub, argv, doc)
    seen = {k: os.environ.get(k) for k in spec.get("record_env", [])}
    call = {"argv": argv, "env": seen, "env_keys": sorted(os.environ), "cwd": os.getcwd(), "sql": sql, "export": export}
    with open(HERE / "calls.jsonl", "a") as log:
        log.write(json.dumps(call) + "\n")
    if refusal:
        print(json.dumps({"error": f"config file '{value_of(argv, '--config')}': {refusal}", "exit_class": 1}), file=sys.stderr)
        return 1
    play = {}
    for key in (f"{sub}:{export}:{table}", f"{sub}:{export}", sub):
        if key in spec:
            play = spec[key]
            break
    if sub == "plan" and not play.get("exit"):
        out = value_of(argv, "--output")
        expires = (datetime.now(timezone.utc) + timedelta(hours=spec.get("plan_expires_hours", 24))).isoformat()
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
    for index in range(play.get("stderr_after", 0)):
        print(f"warning: trailing line {index}", file=sys.stderr)
    sys.stderr.flush()
    if play.get("signal"):
        os.kill(os.getpid(), getattr(signal, play["signal"]))
    return play.get("exit", 0)


sys.exit(main())
