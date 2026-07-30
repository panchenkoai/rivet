"""Shared plumbing for rivet's dev tooling, replacing the bash idioms one by one.

Every helper here exists because the bash equivalent had a failure mode we hit
for real in this repo, not because Python is nicer:

* `run()` takes an **argv list**, so nothing is re-parsed by a shell. The bash
  scripts interpolated table names, URLs and file paths into command strings; a
  value containing a space, a quote or a `$` changed the command's shape.
* `Proc.ok` is the **exit status**. `$?` after a pipeline reads the LAST stage,
  so `cmd | head` reported head's success and a failing check read as a pass.
  Several gate cells decided on `grep -qiE "error|failed"` over the transcript
  instead — which fires on a data row containing the word "error" and misses a
  silent non-zero exit.
* names are **function-local by construction**. macOS bash 3.2 expands a
  same-line `${x}` inside `local x=$1 y="…${x}…"` against the ENCLOSING scope, so
  such a line silently took a caller's value where one existed and, under
  `set -u`, aborted the function where none did. That single line disabled the
  release gate's whole CDC layer; the same pattern appeared three more times.
* `require()` / `Fail` make "tool absent" an explicit outcome. `command -v x ||
  { echo …; exit 1; }` is easy to write and easy to forget, and a `case` with no
  default arm returns 0 — reporting a service UP without probing it.
* `atomic_write()` writes a temp file and renames. A generator that redirects
  straight into its output (`gen > file`) truncates that file before the
  generator can fail, so a crash leaves a half-written artifact that the next
  run's diff then reports as legitimate drift.

Standard library only: these scripts run on a fresh checkout, on CI runners and
inside minimal containers, so a pip install is not available to them.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence, TypeVar

ROOT = Path(__file__).resolve().parents[2]

T = TypeVar("T")


class Fail(Exception):
    """A fatal, EXPECTED condition — a missing tool, an unreachable service, a
    failed check. `main()` turns it into a message and an exit code, so a script
    never has to choose between `exit 1` and an unhandled traceback."""

    def __init__(self, message: str, *, code: int = 1, hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.hint = hint


# ── output ─────────────────────────────────────────────────────────────────────
_COLOUR = sys.stderr.isatty() or os.environ.get("FORCE_COLOR") == "1"


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOUR else text


def log(msg: str, *, tag: str = "dev") -> None:
    """A phase line. Goes to stderr so a script's stdout stays its DATA — the
    bash scripts mixed progress into stdout, so piping one into a file captured
    the chatter along with the payload."""
    print(_c("1;34", f"[{tag}]") + f" {msg}", file=sys.stderr, flush=True)


def ok(msg: str) -> None:
    print(_c("1;32", f"  ✓ {msg}"), file=sys.stderr, flush=True)


def bad(msg: str) -> None:
    print(_c("1;31", f"  ✗ {msg}"), file=sys.stderr, flush=True)


def skip(msg: str) -> None:
    print(_c("1;33", f"  ⊘ {msg}"), file=sys.stderr, flush=True)


def warn(msg: str) -> None:
    print(_c("1;33", f"  ! {msg}"), file=sys.stderr, flush=True)


# ── processes ──────────────────────────────────────────────────────────────────
@dataclass
class Proc:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def out(self) -> str:
        """stdout+stderr, for the cases that genuinely want the transcript.
        Deciding a check on this instead of `ok` is the mistake documented above —
        use it for MESSAGES, not for verdicts."""
        return self.stdout + self.stderr

    def check(self, what: str) -> "Proc":
        """Raise `Fail` unless this succeeded — the `set -e` behaviour, but with
        the command and its stderr in the message instead of a bare exit code."""
        if not self.ok:
            detail = (self.stderr or self.stdout).strip().splitlines()
            tail = detail[-1] if detail else f"exit {self.returncode}"
            raise Fail(f"{what} failed: {tail}")
        return self


def run(
    argv: Sequence[str],
    *,
    stdin: str | None = None,
    timeout: float | None = 600,
    env: dict[str, str] | None = None,
    cwd: Path | str | None = None,
    quiet: bool = True,
) -> Proc:
    """Run `argv` with no shell involved.

    `timeout=None` for genuinely long work (a fat-LTO build, a soak run): a
    timeout kill surfaces as returncode 124, and a check that cannot tell "slow"
    from "broken" fails the wrong thing.
    """
    full_env = {**os.environ, **(env or {})}
    try:
        p = subprocess.run(
            [str(a) for a in argv],
            input=stdin,
            capture_output=quiet,
            text=True,
            timeout=timeout,
            env=full_env,
            cwd=str(cwd) if cwd else None,
        )
        return Proc(tuple(map(str, argv)), p.returncode, p.stdout or "", p.stderr or "")
    except subprocess.TimeoutExpired as e:
        def _s(v: object) -> str:
            if isinstance(v, bytes):
                return v.decode(errors="replace")
            return v if isinstance(v, str) else ""
        return Proc(tuple(map(str, argv)), 124, _s(e.stdout),
                    _s(e.stderr) + f"\n[timed out after {timeout}s]")
    except FileNotFoundError as e:
        return Proc(tuple(map(str, argv)), 127, "", str(e))


def stream(argv: Sequence[str], **kw) -> Proc:
    """Like `run`, but the child's output goes straight to the terminal — for the
    long interactive steps where watching progress IS the point."""
    return run(argv, quiet=False, **kw)


def docker(*args: str, **kw) -> Proc:
    return run(["docker", *args], **kw)


def compose(*args: str, file: Path | None = None, **kw) -> Proc:
    """`docker compose` (v2 plugin form). The bash scripts mixed `docker-compose`
    and `docker compose`; only one of them exists on a modern install."""
    pre = ["docker", "compose"]
    if file is not None:
        pre += ["-f", str(file)]
    return run([*pre, *args], **kw)


def docker_exec(container: str, *args: str, stdin: str | None = None, **kw) -> Proc:
    """`docker exec` on a container, passing every argument through.

    The `-i` flag is added only when there is stdin to send. A bash wrapper that
    dropped `"$@"` here once swallowed a `-c "INSERT …"`, so a crash-delta row was
    never written and the at-least-once check "passed" against data that had never
    been inserted.
    """
    flags = ["-i"] if stdin is not None else []
    return docker("exec", *flags, container, *args, stdin=stdin, **kw)


# ── preconditions ──────────────────────────────────────────────────────────────
def have(tool: str) -> bool:
    return shutil.which(tool) is not None


def require(tool: str, *, hint: str | None = None) -> str:
    """The tool's path, or `Fail` naming how to get it."""
    path = shutil.which(tool)
    if path is None:
        raise Fail(f"`{tool}` not found on PATH", hint=hint)
    return path


def require_env(name: str, *, hint: str | None = None) -> str:
    v = os.environ.get(name)
    if not v:
        raise Fail(f"${name} is not set", hint=hint)
    return v


def wait_until(
    check: Callable[[], bool], *, tries: int = 45, delay: float = 2.0, what: str = "condition"
) -> bool:
    """Poll until true. Returns False on exhaustion rather than raising, so the
    caller decides whether a timeout is a SKIP or a FAIL — the distinction the
    gate's whole SKIP-is-never-a-pass rule rests on."""
    for _ in range(tries):
        if check():
            return True
        time.sleep(delay)
    return False


def tcp_open(host: str, port: int, *, timeout: float = 3.0) -> bool:
    """Is something listening? Replaces the `/dev/tcp/$h/$p` bashism, which is
    unavailable in POSIX sh and silently false there."""
    import socket

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def http_up(url: str, *, timeout: float = 3.0) -> bool:
    """Reachability, mirroring `curl -s -o /dev/null`: any HTTP response — 4xx
    included — means the service is UP. An emulator answering `403` to an
    unauthenticated probe is running; only a transport error means down."""
    import urllib.error
    import urllib.request

    try:
        urllib.request.urlopen(url, timeout=timeout).read(0)
        return True
    except urllib.error.HTTPError:
        return True
    except (urllib.error.URLError, OSError):
        return False


# ── files ──────────────────────────────────────────────────────────────────────
def atomic_write(path: Path, text: str) -> None:
    """Write via a temp file + rename, so a failure cannot leave a truncated
    artifact behind — the `generator > file` shape truncates before it writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def rm_rf(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


# ── entry point ────────────────────────────────────────────────────────────────
def main(fn: Callable[[], int | None]) -> None:
    """Run `fn` as a script body: `Fail` becomes a message + exit code, Ctrl-C
    becomes 130, anything else keeps its traceback (a real bug should be loud)."""
    try:
        raise SystemExit(fn() or 0)
    except Fail as e:
        bad(e.message)
        if e.hint:
            print(f"      {e.hint}", file=sys.stderr)
        raise SystemExit(e.code)
    except KeyboardInterrupt:
        raise SystemExit(130)
