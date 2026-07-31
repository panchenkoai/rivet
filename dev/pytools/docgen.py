#!/usr/bin/env python3
"""Port of `dev/docgen/generate.sh` — regenerate the code-derived reference docs.

The single source of truth is the CODE: the JSON Schema `schemars` emits from the
`Config` types, and the clap `Cli` derive that also produces `--help`. This module
renders both into `docs/reference/`, and `--check` is the CI gate that fails when
the committed copies have drifted from what the code would produce now.

    python3 -m dev.pytools.docgen           # regenerate in place
    python3 -m dev.pytools.docgen --check   # fail if the committed docs are stale

The `.py` helpers under `dev/docgen/` (`config_md.py`, `expand_includes.py`) are
NOT reimplemented here — they are invoked as the separate programs they already
are, so this port cannot fork their behaviour.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:  # `python3 -m dev.pytools.docgen`
    from . import shell
except ImportError:  # `python3 dev/pytools/docgen.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail

DOCGEN = ROOT / "dev" / "docgen"
EXPAND_INCLUDES = DOCGEN / "expand_includes.py"
CONFIG_MD = DOCGEN / "config_md.py"

# `sys.executable`, not a bare "python3": the helpers must run under the same
# interpreter as this module, not whichever python3 happens to be first on PATH.
PY = sys.executable or "python3"

USAGE = """Usage: python3 -m dev.pytools.docgen [--check]

Regenerate the code-derived reference docs (clap + schemars).

  (no args)   regenerate in place
  --check     fail if the committed generated docs are stale"""


@dataclass(frozen=True)
class Generated:
    """One generated doc: where it lands, and the pipeline that produces it.

    `argv` writes the document to stdout; `filter_argv`, when present, is fed
    that stdout — the shell's `producer | filter`, minus the shell.
    """

    out: str  # repo-relative, as the shell listed it and as git wants it
    argv: tuple[str, ...]
    filter_argv: tuple[str, ...] = ()


def generators(rivet: str) -> tuple[Generated, ...]:
    """The generated-docs list — the SINGLE source of truth for both modes.

    The shell kept two lists: the `gen` calls, and a `GENERATED` string re-naming
    the same two files for the `git diff`. Nothing tied them together, so adding
    a third doc to one and not the other would have produced a gate that
    regenerates a file it never checks (or checks one it never regenerates).
    Here `GENERATED` is derived from this list, so the two cannot drift.
    """
    return (
        # Config reference — from the JSON Schema the binary emits (schemars ← Config).
        Generated(
            out="docs/reference/config-reference.md",
            argv=(rivet, "schema", "config"),
            filter_argv=(PY, str(CONFIG_MD)),
        ),
        # CLI reference — from the clap `Cli` derive, the same source as `--help`.
        Generated(out="docs/reference/cli-reference.md", argv=(rivet, "schema", "cli")),
    )


def rivet_bin() -> str:
    """The binary to interrogate: `$RIVET`, else the debug build.

    Treated as a single executable path, not a command line — the shell's
    `[ -x "$RIVET" ]` already rejected anything multi-word, so a `RIVET="cargo run
    …"` never worked there either.
    """
    rivet = os.environ.get("RIVET") or "./target/debug/rivet"
    path = (ROOT / rivet) if not Path(rivet).is_absolute() else Path(rivet)
    if not (path.is_file() and os.access(path, os.X_OK)):
        raise Fail("build the binary first: cargo build --bin rivet")
    return str(path)


def _produce(g: Generated) -> str:
    """Run one generator pipeline and return the document text.

    Both stages are checked. The shell ran the pipeline inside `bash -c "a | b"`,
    a fresh shell WITHOUT the outer `pipefail`, so the pipeline's status was the
    FILTER's alone: a `rivet schema config` that failed after emitting partial
    output would have been recorded as a successful generation. (Today
    `config_md.py` happens to die on truncated JSON, which masks it — that is
    luck, not a guarantee.)
    """
    p = shell.run(list(g.argv), timeout=None, cwd=ROOT)
    p.check(" ".join(g.argv))
    if not g.filter_argv:
        return p.stdout
    q = shell.run(list(g.filter_argv), stdin=p.stdout, timeout=None, cwd=ROOT)
    q.check(" ".join(g.filter_argv))
    return q.stdout


def _write(g: Generated, text: str) -> None:
    """Write the document, then report it as the shell did.

    `atomic_write` is the temp-file + rename the shell did by hand
    (`"$@" > "$out.tmp" && mv`), and for the same reason: redirecting straight
    into the output truncates it BEFORE the generator has produced anything, so a
    generator crash leaves a half-written doc that the next `--check` reports as
    legitimate drift. Unlike the shell's version, a failed generator here leaves
    no `.tmp` behind, because `_produce` raises before any file is touched.
    """
    out = ROOT / g.out
    shell.atomic_write(out, text)
    lines = text.count("\n")  # == `wc -l`: newlines, not "visible lines"
    print(f"generated {g.out} ({lines} lines)", flush=True)


def generate(check: bool = False) -> int:
    """`generate.sh` / `generate.sh --check`. Returns the process exit code."""
    rivet = rivet_bin()
    gens = generators(rivet)

    # Both modes regenerate first — the check is defined as "what the code
    # produces now vs what is committed", so it needs the fresh output on disk.
    for g in gens:
        _write(g, _produce(g))

    paths = [g.out for g in gens]

    if not check:
        # Shared prose fragments (docs/shared/*) expanded into their consumers.
        return shell.stream([PY, str(EXPAND_INCLUDES)], cwd=ROOT, timeout=None).returncode

    rc = shell.stream([PY, str(EXPAND_INCLUDES), "--check"], cwd=ROOT, timeout=None).returncode
    if rc != 0:
        # `set -e` propagated the include checker's own status; keep it, so its
        # `::error::stale doc includes` verdict is what CI reports.
        return rc

    # NOTE on what this gate can and cannot mean: it diffs the WORKING TREE
    # against git HEAD, so it only says anything on a COMMITTED tree. Run it
    # after regenerating but before committing and it reports "stale" — that is
    # the gate working as designed (the regeneration is not committed yet), not a
    # bug. In CI the tree is a fresh checkout, so a diff can only come from
    # generated output that disagrees with what was committed.
    diff = shell.run(["git", "diff", "--quiet", "--", *paths], cwd=ROOT)
    if not diff.ok:
        # Any non-zero counts as stale, exactly as `if ! git diff --quiet` did.
        # (git returns 1 for "differs" and >1 for a real error — e.g. not a
        # repository — and the shell conflated the two. Preserved: a gate that
        # fails loudly on a broken git is the safe direction.)
        print(
            "::error::generated docs are stale — run 'python3 -m dev.pytools.docgen' and commit",
            flush=True,
        )
        shown = shell.run(["git", "--no-pager", "diff", "--", *paths], cwd=ROOT)
        lines = shown.stdout.split("\n")
        if lines and lines[-1] == "":
            lines.pop()  # a trailing newline is not a final blank line
        for line in lines[:60]:  # | head -60
            print(line, flush=True)
        return 1

    print("docs-gen: up to date", flush=True)
    return 0


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    check = False
    for a in args:
        if a == "--check":
            check = True
        elif a in ("-h", "--help"):
            print(USAGE)
            return 0
        else:
            # DEVIATION: the shell compared only `"${1:-}" = "--check"`, so any
            # other argument — including a typo like `-check` — silently ran the
            # REGENERATE path. A CI gate that quietly stops gating on a typo is
            # the worst failure mode available to it, so unknown arguments are
            # now fatal.
            print(f"Unknown argument: {a}", file=sys.stderr)
            print(USAGE, file=sys.stderr)
            return 2
    return generate(check=check)


if __name__ == "__main__":
    shell.main(lambda: main_cli())
