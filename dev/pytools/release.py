"""Tag a Rivet release and (optionally) push the tag — port of `dev/scripts/release.sh`.

Pushing the tag is what triggers `.github/workflows/release.yml`, which builds
the four target tarballs, publishes the crate, writes `SHA256SUMS.txt`, and
regenerates the Homebrew formula. Everything this module does happens BEFORE
that point, so every check here is a gate on an immutable artifact: once the tag
is pushed, a bad tag can only be fixed by a new patch release.

Preconditions the script enforces, in order (unchanged from the bash):

1. `Cargo.toml` has a parseable `version`.
2. `--tag`, when given, looks like `vX.Y.Z` AND matches that version.
3. `CHANGELOG.md` exists and has a `## <version> ` section (the release
   workflow extracts the release notes from exactly that heading — no section
   means an empty release body, and the workflow fails at that step *after* the
   tag is already public).
4. The tag does not already exist locally.
5. The worktree is clean (tracked files only — see the notes below).

WHAT THIS SCRIPT DOES **NOT** CHECK (reported, not invented):

* The `cargo chef prepare` strict-TOML parse that broke 0.16.0's Docker images
  at the tag. That guard exists, but as an offline test —
  `tests/offline/cargo_manifest_chef.rs` — not here.
* The committed-`Cargo.lock` / `cargo publish --locked` sync that aborted
  0.16.1 after the tag was cut. That guard is a CI step
  (`.github/workflows/ci.yml`, `cargo metadata --locked`), deliberately NOT a
  runtime file read, because `cargo` reconciles the working-tree lock before any
  in-process check could observe the staleness.
* Anything `gh`-shaped: the bash script never invoked `gh`, so neither does this.

Both belong to the release path but sit in other layers; this port keeps its
behaviour identical rather than growing new refusals. If they should gate the
tag too, that is a deliberate follow-up, not a silent addition here.

Usage mirrors the bash exactly:

    python dev/pytools/release.py --dry-run
    python dev/pytools/release.py
    python dev/pytools/release.py --push
    python dev/pytools/release.py --message "summary" --push
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/release.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
CARGO_TOML = ROOT / "Cargo.toml"
CHANGELOG = ROOT / "CHANGELOG.md"

USAGE = """Usage: python3 -m dev.pytools.release [--dry-run] [--push] [--tag vX.Y.Z] [--message TEXT]

Options:
  --dry-run     Validate only; do not create or push a tag
  --push        Push the tag to origin after creating it
  --tag vX.Y.Z  Override tag (default: v<version from Cargo.toml>)
  --message     Annotated tag message (default: vX.Y.Z: <CHANGELOG subtitle>)
  -h, --help    Show this help"""

# The bash hard-codes this owner in the "watch:" line even though the Homebrew
# generator derives owner/repo from Cargo.toml. Kept verbatim so the transcript
# diffs clean; flagged rather than silently "improved".
ACTIONS_URL = "https://github.com/panchenkoai/rivet/actions/workflows/release.yml"


class _Usage(Exception):
    """`usage()` in the bash: print the help and exit 1 — note it exits 1 even
    for `-h`, which argparse would not do. Raised so the parse helpers can bail
    from anywhere without a second return channel."""


@dataclass
class Args:
    dry_run: bool = False
    push: bool = False
    tag_override: str = ""
    message: str = ""


def parse_args(argv: list[str]) -> Args:
    """Hand-rolled to match the bash `while/case` loop exactly.

    argparse is avoided on purpose here: it exits 0 on `-h` and 2 on a bad flag,
    while the bash exits 1 for both, and it would accept abbreviations the bash
    never did. A release gate is the wrong place to change an exit code.
    """
    a = Args()
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--dry-run":
            a.dry_run = True
            i += 1
        elif arg == "--push":
            a.push = True
            i += 1
        elif arg == "--tag":
            # `${2:-}` then a non-empty test: a missing OR empty value is usage.
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.tag_override = value
            i += 2
        elif arg == "--message":
            value = argv[i + 1] if i + 1 < len(argv) else ""
            if not value:
                raise _Usage()
            a.message = value
            i += 2
        elif arg in ("-h", "--help"):
            raise _Usage()
        else:
            print(f"Unknown option: {arg}", file=sys.stderr)
            raise _Usage()
    return a


# ── Cargo.toml / CHANGELOG parsing ─────────────────────────────────────────────
_VERSION_LINE = re.compile(r"^version\s*=")
_VERSION_VALUE = re.compile(r'^version\s*=\s*"([^"]*)"')


def cargo_version(cargo_toml: Path = CARGO_TOML) -> str:
    """First column-0 `version = "…"` in Cargo.toml, like `grep | head -1 | sed`.

    Same window as the bash — the FIRST such line wins, which is the `[package]`
    version only because `[package]` is the first table in this manifest. A
    workspace-style `version = { workspace = true }` has no quoted value, so the
    extraction finds nothing and we fail loudly instead of tagging `v`.

    (The bash pipeline `grep … | head -1 | sed …` is also a live `pipefail`
    hazard: `head` closing the pipe early can leave `grep` killed by SIGPIPE,
    which `set -o pipefail` propagates as 141 and `set -e` turns into a silent,
    message-less abort. Reading the file once removes that failure mode.)
    """
    if not cargo_toml.is_file():
        # The bash does not test for this file (the Homebrew script does); it
        # would die inside the pipeline with grep's own message and exit 2. Same
        # refusal, one clear message.
        raise shell.Fail(
            "error: could not parse version from Cargo.toml",
            hint=f"{cargo_toml} does not exist",
        )
    for line in cargo_toml.read_text().splitlines():
        if _VERSION_LINE.match(line):
            m = _VERSION_VALUE.match(line)
            return m.group(1) if m else ""
    return ""


def changelog_has_section(version: str, changelog: Path = CHANGELOG) -> bool:
    """Is there a `## <version> ` heading?

    The awk built its regex by string concatenation, so the version's dots were
    wildcards. We escape them: a heading that only matched through a wildcard was
    never the right heading, so escaping can turn a false PASS into a fail, never
    the reverse — the safe direction for a release gate.
    """
    pattern = re.compile(r"^## " + re.escape(version) + " ")
    for line in changelog.read_text().splitlines():
        if pattern.match(line):
            return True
    return False


# The three heading shapes the awk strips, applied in order to the same line.
# NOTE (faithful bug-for-bug): `[0-9.]+` does not match a pre-release version
# such as `0.2.0-beta.1`, so for those none of the three substitutions fire and
# the ENTIRE heading line becomes the "subtitle" — the tag message reads
# `v0.2.0-beta.1: ## 0.2.0-beta.1 — …`. Reproduced deliberately; see the report.
_SUB_PATTERNS = (
    re.compile(r"^## [0-9.]+ \(unreleased\) — "),
    re.compile(r"^## [0-9.]+ — "),
    re.compile(r"^## [0-9.]+ "),
)


def changelog_subtitle(version: str, changelog: Path = CHANGELOG) -> str:
    """The `## <version> ` heading with its version prefix stripped.

    With the current CHANGELOG shape (`## 0.24.0 — 2026-07-29`) this yields the
    DATE, so the default tag message is `v0.24.0: 2026-07-29`. That is what the
    bash produces today; pass `--message` for a real summary.
    """
    pattern = re.compile(r"^## " + re.escape(version) + " ")
    for line in changelog.read_text().splitlines():
        if not pattern.match(line):
            continue
        text = line
        for sub in _SUB_PATTERNS:
            text = sub.sub("", text, count=1)
        # `$(awk …)` strips trailing newlines but NOT trailing spaces, and the
        # emptiness test below is `-n`, so a space-only subtitle stays truthy.
        return text
    return ""


# ── git ────────────────────────────────────────────────────────────────────────
def _git(*args: str) -> shell.Proc:
    return shell.run(["git", "-C", str(ROOT), *args])


def _git_stream(*args: str) -> shell.Proc:
    """For the two mutating commands, so `git push`'s progress stays visible."""
    return shell.stream(["git", "-C", str(ROOT), *args])


def tag_exists(tag: str) -> bool:
    """`git rev-parse <tag>` — as in the bash, this resolves ANY ref or object,
    so a branch named `v0.24.0` also counts as "already exists". Kept: the
    over-broad refusal blocks a release, it never lets a bad one through."""
    return _git("rev-parse", tag).ok


def worktree_dirty() -> bool:
    """Unstaged or staged changes. Untracked files are NOT considered dirty —
    same as the bash, and defensible (nothing untracked ends up in the tag), but
    worth knowing: a new-but-unadded source file will not block a release."""
    return not _git("diff", "--quiet").ok or not _git("diff", "--cached", "--quiet").ok


# ── main ───────────────────────────────────────────────────────────────────────
def main_cli(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(list(sys.argv[1:] if argv is None else argv))
    except _Usage:
        print(USAGE)
        return 1

    # `git` was never checked in the bash — it would have died at the first call
    # with a shell "command not found". Same refusal, named.
    shell.require("git", hint="install git, then re-run")

    version = cargo_version()
    if not version:
        raise shell.Fail("error: could not parse version from Cargo.toml")

    if args.tag_override:
        tag = args.tag_override
        if tag.startswith("v"):
            tag_version = tag[1:]
        else:
            raise shell.Fail(f"error: --tag must look like vX.Y.Z (got: {tag})")
        # The tag and the crate version must agree: the workflow builds from the
        # tagged tree and names every asset after the TAG, while crates.io
        # publishes the Cargo.toml VERSION. A mismatch ships a formula whose
        # URLs and crate version disagree.
        if tag_version != version:
            raise shell.Fail(
                f"error: Cargo.toml version is {version}, but --tag is {tag}"
            )
    else:
        tag = f"v{version}"

    if not CHANGELOG.is_file():
        raise shell.Fail("error: CHANGELOG.md not found")

    if not changelog_has_section(version):
        raise shell.Fail(
            f"error: CHANGELOG.md has no section starting with '## {version} '"
        )

    if tag_exists(tag):
        raise shell.Fail(f"error: tag {tag} already exists locally")

    if worktree_dirty():
        raise shell.Fail(
            "error: working tree is not clean — commit or stash before tagging"
        )

    head = _git("rev-parse", "--short", "HEAD").check("git rev-parse HEAD").stdout.strip()
    branch = (
        _git("rev-parse", "--abbrev-ref", "HEAD")
        .check("git rev-parse --abbrev-ref HEAD")
        .stdout.strip()
    )

    message = args.message
    if not message:
        subtitle = changelog_subtitle(version)
        message = f"{tag}: {subtitle}" if subtitle else tag

    print(
        f"""Rivet release
  tag:     {tag}
  version: {version}
  commit:  {head} ({branch})
  message: {message}
  push:    {"yes" if args.push else "no"}
  CI:      .github/workflows/release.yml (on tag push)"""
    )

    if args.dry_run:
        print()
        print("Dry run — no tag created.")
        return 0

    _git_stream("tag", "-a", tag, "-m", message).check(f"git tag -a {tag}")
    print(f"Created tag {tag}")

    if args.push:
        # The point of no return: the workflow starts here.
        _git_stream("push", "origin", tag).check(f"git push origin {tag}")
        print(f"Pushed {tag} — watch: {ACTIONS_URL}")
    else:
        print()
        print(f"Next: git push origin {tag}")

    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
