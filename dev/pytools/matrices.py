#!/usr/bin/env python3
"""Port of `dev/matrices/run.sh` + `dev/matrices/_common/lib/check_msg.sh`.

Two things live here because they are one family: the orchestrator that runs the
regression matrix harnesses, and the substring-contract checker those harnesses
(`cli_matrix/check_msg.sh`, `cfg_matrix/check_msg.sh`) delegate to. Nothing else
uses the checker, so it does not earn a module of its own.

    python3 -m dev.pytools.matrices [--tier=pr|nightly|release|all]
                                    [--matrix=cli,cfg,...] [--build]
                                    [--skip-compose]

The bash originals are reproduced faithfully — same scenarios, same order, same
messages, same exit codes — except where the shell silently turned a broken run
into a pass. Each of those is marked `DEVIATION:` at its site, and they are:

1. An unknown `--tier` exited **0** with "OK: all requested matrices passed."
   `tier_list` was called inside `$(...)`, and `exit 2` in a command
   substitution kills only the subshell — the parent read an empty string,
   `MATRICES` became an empty array, every loop body was skipped and the run
   congratulated itself. (Verified: bash 5.3 exits 0 here. On the macOS bash 3.2
   the empty array trips `set -u` instead, aborting with `MATRICES[@]: unbound
   variable` — a different wrong answer per platform.) `--matrix=<typo>` has the
   same shape: it reached `stage_rivet.sh` with an empty directory argument.
2. Seed and staging failures were unchecked (`run.sh` has no `set -e`), so a
   matrix could run against un-seeded tables or a stale/absent `./rivet` and
   still report success. Both now count.
3. The "Matrix directory missing — run setup_links.sh" guard was unreachable:
   `stage_rivet.sh` does `mkdir -p "$dir"` and ran first, so a missing symlink
   got papered over with a stub directory holding a copied binary, and the `-d`
   test meant to catch it then passed. The check now runs before any staging,
   while it can still be true.
4. `REPO` was `dirname($0)/..` = `dev/`, not the repo root, so `docker compose`
   worked only via Compose v2's parent-directory search — while `wait_postgres`
   ran `docker compose` in whatever directory the *caller* happened to be in.
   Everything now runs against the repo root, where `docker-compose.yaml` is.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

try:  # `python3 -m dev.pytools.matrices`
    from . import shell
except ImportError:  # `python3 dev/pytools/matrices.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
bad, log, ok = shell.bad, shell.log, shell.ok
compose, stream = shell.compose, shell.stream

MATRICES_DIR = ROOT / "dev" / "matrices"
COMMON = MATRICES_DIR / "_common"

# Tier → matrices, in run order. `release` and `all` hold the same list rather
# than aliasing, because the bash arm was shared and any future divergence
# should be an explicit edit here.
TIERS: dict[str, tuple[str, ...]] = {
    "pr": ("cli", "cfg", "path"),
    "nightly": ("cli", "cfg", "path", "query", "soak"),
    "release": ("cli", "cfg", "path", "query", "soak", "cross_version"),
    "all": ("cli", "cfg", "path", "query", "soak", "cross_version"),
}

# Matrix → directory under dev/matrices (each a symlink into dev/<name>_matrix,
# created by setup_links.sh). The path is kept symlinked, not resolved: the
# harnesses reach back out with `$ROOT/../matrices/...`, which the kernel
# resolves through the link to the real parent — exactly as under bash.
MATRIX_DIRS: dict[str, str] = {
    "cli": "surface/cli",
    "cfg": "surface/cfg",
    "path": "execution/path",
    "query": "execution/query",
    "soak": "resources/soak",
    "cross_version": "compatibility/cross_version",
}

# Services each matrix needs, mirroring `_common/compose_profiles.yaml`. The bash
# encoded this a second time in an `ensure_services` case arm with NO default, so
# an unrecognised matrix quietly started nothing and then waited for nothing.
NEEDS_PG_AND_MYSQL: frozenset[str] = frozenset({"cli", "cfg", "cross_version"})
NEEDS_PG_ONLY: frozenset[str] = frozenset({"path", "query", "soak"})
NEEDS_LEGACY: frozenset[str] = frozenset({"cross_version"})

LEGACY_SERVICES: tuple[str, ...] = (
    "postgres",
    "postgres-12",
    "postgres-13",
    "postgres-14",
    "postgres-15",
    "mysql",
    "mysql-57",
)

USAGE = """Usage: python3 -m dev.pytools.matrices [OPTIONS]
       python3 -m dev.pytools.matrices check-msg --contract FILE --logs DIR [--layout flat|probe]

Run regression matrix harnesses grouped under dev/matrices/.

Options:
  --tier=pr|nightly|release|all   Which matrices to run (default: all)
  --matrix=cli,cfg,...            Run only named matrices (overrides --tier)
  --build                         Force cargo build --release before run
  --skip-compose                  Assume docker services already up
  -h, --help                      Show this help

Subcommand:
  check-msg                       Grade captured logs against a substring
                                  contract (`expected_msg.txt`). The tier run
                                  does this itself; this entry point exists to
                                  re-grade an existing `logs/` without
                                  re-running the probes.

Matrices: cli, cfg, path, query, soak, cross_version

Tiers (see _common/compose_profiles.yaml):
  pr        cli, cfg, path
  nightly   pr + query, soak
  release   nightly + cross_version
  all       same as release"""


# ── check_msg.sh ───────────────────────────────────────────────────────────────
# A substring contract: one assertion per line, checked against the per-scenario
# log files a matrix captured.
#
#   flat:   <id> <stream> <op> <substring...>          (cli_matrix)
#   probe:  <id> <probe>/<stream> <op> <substring...>  (cfg_matrix)
#
# ops: `+` must contain, `-` must not, `=N` exactly N matching lines.

# POSIX [[:space:]] in the C locale, which is what the original awk/sed saw — NOT
# Python's `\s`, which also matches non-ASCII whitespace and would therefore cut
# a substring the shell kept intact.
_WS = r"[ \t\r\v\f]"
_NON_WS = r"[^ \t\r\v\f]"
# Strip exactly the first three whitespace-delimited tokens, byte-verbatim. The
# substring runs to end-of-line and may legitimately contain runs of spaces
# (column-aligned validator output like `status:    PASSED`), which is why the
# original reached for sed instead of awk's field reassembly — awk collapses them.
_STRIP_THREE = re.compile(rf"^{_WS}*{_NON_WS}+{_WS}+{_NON_WS}+{_WS}+{_NON_WS}+{_WS}+")


@dataclass(frozen=True)
class Assertion:
    """One parsed contract line."""

    raw: str
    id: str
    col2: str
    op: str
    substr: str


def parse_contract_line(raw: str) -> Assertion | None:
    """Parse one contract line; `None` for a blank or comment-only line.

    Faithful to the shell, including its sharp edge: `${raw%%#*}` truncates at
    the FIRST `#`, so a substring containing `#` silently becomes a shorter — and
    therefore weaker — assertion. No contract in the repo does that today; a
    future one that tries will not be told why it stopped matching.
    """
    line = raw.split("#", 1)[0].rstrip(" \t\r\v\f")
    if not line:
        return None
    fields = line.split()
    m = _STRIP_THREE.match(line)
    return Assertion(
        raw=raw,
        id=fields[0] if len(fields) > 0 else "",
        col2=fields[1] if len(fields) > 1 else "",
        op=fields[2] if len(fields) > 2 else "",
        # DEVIATION: with fewer than four tokens the sed did not match and
        # `substr` fell back to the WHOLE line — non-empty, so the malformed
        # check never fired and the harness grepped for a nonsense literal
        # ("id stream +"). It reported NOT FOUND, i.e. failed by luck; had any
        # log line ever contained that text, the broken contract line would have
        # PASSED. An unmatched strip is now MALFORMED, which is what it is.
        substr=line[m.end() :] if m else "",
    )


def _read_log(path: Path) -> str:
    """Read a captured log the way grep does — as bytes.

    `surrogateescape` round-trips invalid UTF-8 (a truncated multi-byte sequence
    from a killed process, a binary blob in a transcript), so substring tests
    stay byte-exact instead of raising or lossily substituting U+FFFD.
    """
    return path.read_bytes().decode("utf-8", errors="surrogateescape")


def expect_message(
    actual: str,
    expected: str,
    *,
    what: str,
    op: str = "+",
    id_: str | None = None,
) -> bool:
    """One contract assertion against one captured stream. True = it holds.

    `actual` is the whole captured text; matching is per LINE because the shell
    used `grep -F`. `=N` therefore counts matching LINES, not occurrences, even
    though the message says "occurrences" — kept as-is, since the committed
    contracts were written against the counting that ran, not against the wording.

    Diagnostics go to stderr in the original's exact wording and column
    alignment, so a transcript diff between the two implementations is empty.
    `id_` exists only because the BAD OP arm printed the scenario id where every
    other arm printed the full label.
    """
    lines = actual.split("\n")  # not splitlines(): that also splits \r, \f, \v
    if op == "+":
        if not any(expected in ln for ln in lines):
            print(
                f"NOT FOUND  {what}: expected substring not present: {expected}",
                file=sys.stderr,
            )
            return False
        return True
    if op == "-":
        hits = [ln for ln in lines if expected in ln]
        if hits:
            print(f"PRESENT    {what}: forbidden substring found: {expected}", file=sys.stderr)
            print(f"           line: {hits[0]}", file=sys.stderr)
            return False
        return True
    if op.startswith("="):
        want = op[1:]
        got = sum(1 for ln in lines if expected in ln)
        # String comparison, as in bash: `=1` matches "1", `=01` never can, and a
        # non-numeric `=abc` always fails rather than erroring.
        if str(got) != want:
            print(
                f"COUNT      {what}: expected {want} occurrences, got {got}: {expected}",
                file=sys.stderr,
            )
            return False
        return True
    print(
        f"BAD OP     {id_ if id_ is not None else what}: "
        f"unknown op '{op}' (expected +, -, or =N)",
        file=sys.stderr,
    )
    return False


def check_contract(contract: Path, logs: Path, *, layout: str = "flat") -> int:
    """Check every assertion in `contract` against the logs under `logs`.

    Returns the process exit code: 0 only when nothing failed AND nothing was
    missing — an uncaptured scenario is not a pass, it is an absent measurement.
    Precondition failures return 2 with the shell's exact bare wording (rather
    than raising `Fail`, whose decoration would change a transcript a harness
    may well be diffing).
    """
    if layout not in ("flat", "probe"):
        print(f"Unknown layout '{layout}' (expected flat or probe)", file=sys.stderr)
        return 2
    if not contract.is_file():
        print(f"{contract} missing.", file=sys.stderr)
        return 2
    if not logs.is_dir():
        print(f"{logs} missing — run matrix.sh first.", file=sys.stderr)
        return 2

    failures = 0
    missing = 0
    checked = 0

    raw_lines = contract.read_text(errors="surrogateescape").split("\n")
    if raw_lines and raw_lines[-1] == "":
        # A trailing newline is not a final empty line. (The shell's
        # `read … || [[ -n "$raw" ]]` loop drew the same distinction, so that an
        # unterminated last line is still processed.)
        raw_lines.pop()

    for raw in raw_lines:
        a = parse_contract_line(raw)
        if a is None:
            continue

        if not a.id or not a.col2 or not a.op or not a.substr:
            print(f"MALFORMED  {raw}", file=sys.stderr)
            failures += 1
            continue

        if layout == "flat":
            stream_name = a.col2
            file = logs / a.id / stream_name
            label = f"{a.id} {stream_name}"
        else:
            probe = a.col2.split("/", 1)[0]  # ${col2%%/*}
            stream_name = a.col2.rsplit("/", 1)[-1]  # ${col2##*/}
            if probe == stream_name:
                print(
                    f"BAD FMT    {a.id}: expected <probe>/<stream>, got '{a.col2}'",
                    file=sys.stderr,
                )
                failures += 1
                continue
            file = logs / a.id / probe / stream_name
            label = f"{a.id}/{probe}/{stream_name}"

        if not file.is_file():
            print(f"MISSING    {label} not captured", file=sys.stderr)
            missing += 1
            continue

        checked += 1
        if not expect_message(_read_log(file), a.substr, what=label, op=a.op, id_=a.id):
            failures += 1

    if failures == 0 and missing == 0:
        print(f"OK: {checked} assertion(s) hold.", flush=True)
        return 0

    print("", file=sys.stderr)
    print(f"{failures} assertion(s) failed, {missing} scenario(s) missing.", file=sys.stderr)
    print(
        "If the change is intentional: regenerate expected_msg.txt and document in CHANGELOG.",
        file=sys.stderr,
    )
    return 1


def check_msg_main(argv: Sequence[str] | None = None) -> int:
    """`check_msg.sh --contract FILE --logs DIR [--layout flat|probe]`.

    Exposed as an entry point so the per-matrix `check_msg.sh` wrappers can be
    pointed here without a shim.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    contract = ""
    logs = ""
    layout = "flat"
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--contract", "--logs", "--layout"):
            # DEVIATION: a flag with no value tripped `set -u` ("$2: unbound
            # variable", exit 1). A usage error is a usage error: exit 2.
            if i + 1 >= len(args):
                print(f"{a} requires a value", file=sys.stderr)
                return 2
            value = args[i + 1]
            if a == "--contract":
                contract = value
            elif a == "--logs":
                logs = value
            else:
                layout = value
            i += 2
            continue
        if a in ("-h", "--help"):
            print(
                "Usage: python3 -m dev.pytools.matrices check-msg "
                "--contract FILE --logs DIR [--layout flat|probe]",
                file=sys.stderr,
            )
            return 0
        print(f"Unknown argument: {a}", file=sys.stderr)
        return 2

    if not contract or not logs:
        print(
            "check-msg requires --contract and --logs", file=sys.stderr
        )
        return 2
    return check_contract(Path(contract), Path(logs), layout=layout)


# ── run.sh ─────────────────────────────────────────────────────────────────────
@dataclass
class Options:
    tier: str = "all"
    matrices: tuple[str, ...] = ()
    build: bool = False
    skip_compose: bool = False


class _Usage(Exception):
    """Stop parsing: print usage and exit with `code` (0 for --help, 2 for a
    bad argument)."""

    def __init__(self, code: int, message: str | None = None) -> None:
        super().__init__(message or "")
        self.code = code
        self.message = message


def parse_args(argv: Sequence[str]) -> Options:
    """The original's hand-rolled parser: only the `--flag=value` form, and an
    unrecognised argument is fatal (exit 2) rather than ignored."""
    o = Options()
    for a in argv:
        if a.startswith("--tier="):
            o.tier = a.split("=", 1)[1]
        elif a.startswith("--matrix="):
            value = a.split("=", 1)[1]
            # `IFS=',' read -ra` on an empty value yielded NO fields, so a bare
            # `--matrix=` fell through to the tier. Empty elements from `a,,b`
            # were kept, and are kept here too — they now fail loudly.
            o.matrices = tuple(value.split(",")) if value else ()
        elif a == "--build":
            o.build = True
        elif a == "--skip-compose":
            o.skip_compose = True
        elif a in ("-h", "--help"):
            raise _Usage(0)
        else:
            raise _Usage(2, f"Unknown argument: {a}")
    return o


def tier_list(tier: str) -> tuple[str, ...]:
    """Matrices for a tier. An unknown tier is fatal — see deviation 1."""
    try:
        return TIERS[tier]
    except KeyError:
        raise Fail(f"Unknown tier '{tier}'", code=2) from None


def matrix_dir(name: str) -> Path:
    """Directory for a matrix. An unknown name is fatal — see deviation 1."""
    try:
        return MATRICES_DIR / MATRIX_DIRS[name]
    except KeyError:
        raise Fail(f"Unknown matrix '{name}'", code=2) from None


def _compose_probe(service: str, *cmd: str) -> bool:
    # A short per-probe timeout: 30 attempts at the runner's 10-minute default
    # would turn "the docker daemon is wedged" into a five-hour hang.
    return compose("exec", "-T", service, *cmd, cwd=ROOT, timeout=20).ok


def wait_postgres() -> None:
    log("Waiting for Postgres...")
    if not shell.wait_until(
        lambda: _compose_probe("postgres", "pg_isready", "-U", "rivet"), tries=30, delay=2.0
    ):
        raise Fail("Postgres did not become ready in time")
    ok("Postgres ready")


def wait_mysql() -> None:
    log("Waiting for MySQL...")
    if not shell.wait_until(
        lambda: _compose_probe(
            "mysql", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-privet"
        ),
        tries=30,
        delay=2.0,
    ):
        raise Fail("MySQL did not become ready in time")
    ok("MySQL ready")


def compose_up(profile: str, services: Sequence[str], *, skip_compose: bool) -> None:
    if skip_compose:
        return
    args = (["--profile", profile] if profile else []) + ["up", "-d", *services]
    compose(*args, cwd=ROOT, timeout=None, quiet=False).check("docker compose up")


def ensure_services(matrices: Sequence[str], *, skip_compose: bool) -> None:
    """Start (unless `--skip-compose`) and then always WAIT for what the selected
    matrices need. The wait runs either way on purpose: `--skip-compose` asserts
    the services are already up, and an assertion is worth verifying."""
    need_pg = any(m in NEEDS_PG_AND_MYSQL or m in NEEDS_PG_ONLY for m in matrices)
    need_my = any(m in NEEDS_PG_AND_MYSQL for m in matrices)
    need_legacy = any(m in NEEDS_LEGACY for m in matrices)

    if not (need_pg or need_my or need_legacy):
        return

    # An absent docker is a loud precondition failure. Without this the compose
    # probes simply return 127 thirty times and the run blames the database:
    # "Postgres did not become ready in time".
    shell.require("docker", hint="install Docker / OrbStack and start the daemon")

    if need_legacy:
        compose_up("legacy", LEGACY_SERVICES, skip_compose=skip_compose)
    elif need_pg and need_my:
        compose_up("", ["postgres", "mysql"], skip_compose=skip_compose)
    elif need_pg:
        compose_up("", ["postgres"], skip_compose=skip_compose)

    if need_pg:
        wait_postgres()
    if need_my:
        wait_mysql()


def stage_all(matrices: Sequence[str], *, build: bool) -> None:
    """Copy (or build) the release binary into each matrix directory.

    DEVIATION: a staging failure is now fatal. Every harness runs `./rivet` from
    its own directory, so an unstaged binary means the matrix measured nothing —
    the shell ignored the exit code and let the run continue into a stale copy.
    """
    # The PORTED staging, not `stage_rivet.sh`. The `.check()` above it used to be
    # vacuous: that script has no `set -e`, its last statement is an `echo`, and a
    # failed `cp` therefore exits 0 while printing "Staged …" — verified by running
    # it against an unwritable destination. So "a staging failure is now fatal" was
    # checking an exit code the shell never set, and a matrix could measure a stale
    # binary. The port raises instead, and also refuses to `mkdir -p` a missing
    # matrix directory (which is what let the "run setup_links.sh" guard pass on a
    # stub the script had just created).
    from .matrix_common import stage_rivet

    for m in matrices:
        stage_rivet(matrix_dir(m), ROOT, build=build)


def _run_step(label: str, fn: Callable[[], object]) -> bool:
    """Run one harness step IN THIS PROCESS. True = it passed.

    Every harness is a Python function now, so a step is a call rather than a
    `bash <matrix>/matrix.sh`. Two consequences worth naming:

    * A harness that ABORTS (`Fail`) is a failed step carrying its own message,
      instead of an exit code the caller has to re-interpret. The shell could
      only see the number.
    * Only an **int** return is read as a status code — that is the suites'
      convention (`matrix_suites.cli`, `cfg_matrix.run_matrix`, … all return an
      rc). The seeds signal failure by RAISING and return something
      informational instead: `seed_pa_soak` returns the row count as a string,
      which an "anything non-zero is a failure" rule read as `exit 10000` and
      reported a successful seed of 10 000 rows as a failed step.
    """
    print("", file=sys.stderr)
    log(f"=== {label} ===")
    try:
        rc = fn()
    except Fail as e:
        bad(f"{label} failed: {e}")
        return False
    # bool before int: `True`/`False` are ints in Python, and `False == 0` would
    # otherwise read a failed predicate as a clean exit.
    if isinstance(rc, bool):
        passed = rc
    elif isinstance(rc, int):
        passed = rc == 0
    else:
        passed = True  # None, or an informational value from a seed
    if not passed:
        bad(f"{label} failed (exit {rc})")
    return passed


def steps_for(name: str, directory: Path) -> list[tuple[str, Callable[[], object]]]:
    """The ordered (label, step) pairs of one matrix.

    A total mapping rather than a `case` with no default arm: an unknown name
    cannot reach here (names are validated up front), and if one ever does, it
    raises instead of running zero steps and reporting success.

    Each step calls the harness's own module. The paths are still derived from
    `directory` — the symlinked matrix dir — so a harness reads the same `cfg/`,
    `logs/` and contract files it read when this dispatched through bash.
    """
    from . import cfg_matrix, cross_version, matrix_suites
    from .matrix_common import seed_pa_audit, seed_pa_audit_all, seed_pa_soak

    if name == "cli":
        return [
            (f"{name} (seed pa_audit)", seed_pa_audit),
            (name, lambda: matrix_suites.cli(root=str(directory))),
            (f"{name} (check_rc)", lambda: matrix_suites.check_rc(root=str(directory))),
        ]
    if name == "cfg":
        return [
            (
                name,
                lambda: cfg_matrix.run_matrix(
                    cfg_dir=directory / "cfg",
                    logs_dir=directory / "logs",
                    matrix_dir=directory,
                ),
            ),
            (
                f"{name} (check_msg)",
                lambda: check_msg_main(
                    [
                        "--contract",
                        str(directory / "expected_msg.txt"),
                        "--logs",
                        str(directory / "logs"),
                        "--layout",
                        "probe",
                    ]
                ),
            ),
        ]
    if name in ("path", "query"):
        suite = matrix_suites.path if name == "path" else matrix_suites.query
        return [
            (f"{name} (seed pa_audit)", lambda: seed_pa_audit("postgres")),
            (name, lambda: suite(root=str(directory))),
        ]
    if name == "soak":
        steps: list[tuple[str, Callable[[], object]]] = []
        # Fixtures are generated only when absent: regenerating them every run
        # would reshuffle the soak's inputs and make two runs incomparable.
        if not (directory / "cfg").is_dir():
            steps.append(
                (
                    f"{name} (gen_fixtures)",
                    lambda: matrix_suites.gen_fixtures("soak", root=str(directory)),
                )
            )
        steps.append((f"{name} (seed pa_soak)", seed_pa_soak))
        steps.append((name, lambda: matrix_suites.soak(root=str(directory))))
        return steps
    if name == "cross_version":
        return [
            (f"{name} (seed pa_audit, all versions)", seed_pa_audit_all),
            (name, lambda: cross_version.run_matrix(directory)),
            (f"{name} (check_cross)", lambda: cross_version.check_cross(directory)),
        ]
    raise Fail(f"Unknown matrix '{name}'", code=2)


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)

    # `check-msg` first: the contract checker was reachable only from inside a
    # tier run (and `parse_args` rejects `--contract` as unknown), so the command
    # the matrix READMEs hand a reader — "re-grade the logs I already captured" —
    # had no entry point at all. The per-matrix `check_msg.sh` wrappers used to be
    # that entry point; this replaces them without a shim.
    if args and args[0] == "check-msg":
        return check_msg_main(args[1:])

    try:
        o = parse_args(args)
    except _Usage as u:
        if u.message:
            print(u.message, file=sys.stderr)
        print(USAGE, file=sys.stderr if u.code else sys.stdout)
        return u.code

    matrices = list(o.matrices) if o.matrices else list(tier_list(o.tier))

    # Validate every name AND directory BEFORE touching docker, building or
    # staging: these are the two ways the shell turned a typo into either a
    # silent pass or a littered stub directory (deviations 1 and 3).
    dirs: dict[str, Path] = {}
    for m in matrices:
        d = matrix_dir(m)
        if not d.is_dir():
            raise Fail(
                f"Matrix directory missing: {d} "
                "(run 'python3 -m dev.pytools.matrix_common setup-links')",
                code=2,
            )
        dirs[m] = d

    ensure_services(matrices, skip_compose=o.skip_compose)

    if o.build:
        log("cargo build --bin rivet --release")
        stream(["cargo", "build", "--bin", "rivet", "--release"], cwd=ROOT, timeout=None).check(
            "cargo build --release"
        )

    stage_all(matrices, build=o.build)

    failures = 0
    for m in matrices:
        for label, step in steps_for(m, dirs[m]):
            if not _run_step(label, step):
                failures += 1

    print("", file=sys.stderr)
    if failures > 0:
        bad(f"FAILED: {failures} matrix step(s) failed.")
        return 1
    # The verdict stays on stdout, where the bash left it, in case a wrapper
    # greps for it; all the progress chatter above went to stderr.
    print("OK: all requested matrices passed.", flush=True)
    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
