"""Generate `packaging/homebrew/rivet.rb` — port of `dev/scripts/update_homebrew_formula.sh`.

The formula is what `brew install rivet` obeys, so its `sha256` lines ARE the
integrity check for the published tarballs. They therefore come from exactly one
place: the `SHA256SUMS.txt` the release workflow produced (`--sums-file`, the CI
path) or the same file downloaded from the release (`--tag`, the manual path).
They are never recomputed locally — a locally recomputed digest would attest a
local build, not the artifact users download.

Two consequences this module treats as hard rules:

* A missing entry is a `Fail`, never an empty `sha256`. Homebrew treats an empty
  digest as "no expected value" and would install an unverified bottle.
* The four asset basenames must stay in lockstep with the workflow's `Package
  binary` step (`rivet-<tag>-<target>.tar.gz`). A rename there silently breaks
  the lookup here — which is why the lookup fails loudly instead of skipping.

Usage mirrors the bash:

    python dev/pytools/homebrew_formula.py
    GITHUB_REPOSITORY=owner/rivet python dev/pytools/homebrew_formula.py
    python dev/pytools/homebrew_formula.py --tag v0.2.0-beta.1
    python dev/pytools/homebrew_formula.py --tag v0.2.0-beta.1 --sums-file artifacts/SHA256SUMS.txt
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/homebrew_formula.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
CARGO_TOML = ROOT / "Cargo.toml"
OUT = ROOT / "packaging" / "homebrew" / "rivet.rb"

PROG = "dev/pytools/homebrew_formula.py"
USAGE = (
    f"Usage: {PROG} [--tag vX.Y.Z] [--sums-file PATH]\n"
    "  --sums-file   use local SHA256SUMS.txt (skips download; use with --tag in CI)"
)

# Order is load-bearing: the formula's macOS-arm / macOS-intel / linux-arm /
# linux-intel blocks read these positionally, as the bash's ASSETS[0..3] did.
TARGETS = (
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
)

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


class _Usage(Exception):
    """`usage()` in the bash: help to stderr, exit 1 — for `-h` too."""


# ── Cargo.toml ─────────────────────────────────────────────────────────────────
def _first_quoted(key: str, text: str) -> str:
    """First column-0 `<key> = "…"` value, mirroring `grep | head -1 | sed -n`.

    Reading the file once also removes the bash pipeline's `pipefail`/SIGPIPE
    hazard (`head` closing the pipe can leave `grep` killed by signal 13, which
    `pipefail` reports as 141 and `set -e` turns into a message-less abort).
    """
    line_re = re.compile(rf"^{re.escape(key)}\s*=")
    value_re = re.compile(rf'^{re.escape(key)}\s*=\s*"([^"]*)"')
    for line in text.splitlines():
        if line_re.match(line):
            m = value_re.match(line)
            return m.group(1) if m else ""
    return ""


def resolve_repository(repo_url: str = "", env_value: str | None = None) -> str:
    """`owner/repo`: `$GITHUB_REPOSITORY` wins, else Cargo.toml's `repository`.

    The URL→slug reduction is the bash's `sed | sed | tr` chain verbatim: strip a
    `http(s)://github.com/` prefix, strip a trailing `.git`, drop whitespace.
    Note (bash bug carried over, not fixed): a NON-github repository URL survives
    the prefix strip intact, still contains a `/`, and so passes the validity
    test below — yielding `owner = "https:"` and a 404 URL in the formula. It
    fails loudly at `brew install` rather than installing anything unverified, so
    it is reported rather than patched here.
    """
    value = (os.environ.get("GITHUB_REPOSITORY", "") if env_value is None else env_value) or ""
    if not value and repo_url:
        value = re.sub(r"^https?://github\.com/", "", repo_url)
        value = re.sub(r"\.git$", "", value)
        value = re.sub(r"\s", "", value)
    if not value or "/" not in value:
        raise shell.Fail(
            "error: set GITHUB_REPOSITORY=owner/repo or add a valid repository URL to Cargo.toml"
        )
    return value


# ── SHA256SUMS ─────────────────────────────────────────────────────────────────
def parse_sums(text: str) -> dict[str, str]:
    """`{filename: sha256}` from `sha256sum` output.

    Accepts both renderings the bash's two greps between them covered: `<sha>
    <name>` (text mode) and `<sha> *<name>` (binary mode). Lines that are blank
    or lack two fields are ignored, as a grep for a specific asset would ignore
    them.
    """
    sums: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        sha, name = parts[0], parts[1].strip()
        if name.startswith("*"):  # sha256sum binary-mode marker
            name = name[1:]
        if name:
            sums.setdefault(name, sha)
    return sums


def sha_for(sums: dict[str, str], base: str) -> str:
    """The digest for `base`, or `Fail`.

    Exact name first, then a basename match — the workflow's checksums are
    generated with `cd artifacts && sha256sum *.tar.gz`, so the names are bare,
    but a hand-run `sha256sum artifacts/*.tar.gz` prefixes a path (which the
    bash's `grep -F " name"` / `grep -E "[[:space:]]name$"` both MISSED, failing
    a legitimate sums file). An ambiguous basename — two paths, two different
    digests — is a `Fail`, not a coin flip.
    """
    sha = sums.get(base)
    if sha is None:
        matches = {s for name, s in sums.items() if Path(name).name == base}
        if len(matches) > 1:
            raise shell.Fail(
                f"error: conflicting checksums for {base} in SHA256SUMS.txt"
            )
        sha = next(iter(matches), None)
    if not sha:
        raise shell.Fail(f"error: no checksum for {base} in SHA256SUMS.txt")
    if not _SHA256.match(sha):
        # Cheap guard on the one value that carries the whole integrity promise:
        # a malformed digest would be written into the formula verbatim and only
        # noticed by a user's failed install. (Added; the bash printed field 1
        # whatever it was.)
        raise shell.Fail(
            f"error: malformed sha256 for {base} in SHA256SUMS.txt: {sha!r}"
        )
    return sha


def fetch_sums(sums_url: str, *, tag: str, repository: str) -> str:
    """Download the release's `SHA256SUMS.txt`.

    `curl` rather than urllib to keep the bash's proxy/CA behaviour and the same
    "does the release exist?" message. Non-zero for any reason — 404, DNS, no
    curl — is the same refusal; only the hint distinguishes a missing curl, which
    the bash reported as a missing release.
    """
    print(f"Fetching {sums_url} ...")
    p = shell.run(["curl", "-fsSL", sums_url])
    if not p.ok:
        raise shell.Fail(
            f"error: could not download SHA256SUMS (does release {tag} exist on {repository}?)",
            hint="`curl` not found on PATH" if p.returncode == 127 else None,
        )
    return p.stdout


# ── rendering ──────────────────────────────────────────────────────────────────
def render_formula(
    tag: str,
    sums: dict[str, str],
    *,
    repository: str | None = None,
    version: str | None = None,
) -> str:
    """The full `rivet.rb` text.

    Pure when `repository` and `version` are passed (the unit-testable shape);
    with them omitted it resolves them the way the bash did — `$GITHUB_REPOSITORY`
    or Cargo.toml's `repository`, and the tag minus its leading `v`, falling back
    to Cargo.toml's `version` for a tag that does not start with `v`.

    Every digest goes through `sha_for`, so a missing or malformed entry raises
    before any text is produced: there is no path from here to a formula with a
    blank `sha256`.
    """
    if repository is None or version is None:
        cargo = CARGO_TOML.read_text() if CARGO_TOML.is_file() else ""
        if repository is None:
            repository = resolve_repository(_first_quoted("repository", cargo))
        if version is None:
            version = tag[1:] if tag.startswith("v") else _first_quoted("version", cargo)

    # `${x%%/*}` is the first segment and `${x##*/}` the last, so an
    # `owner/group/repo` value collapses the same way it did in bash.
    owner = repository.partition("/")[0]
    repo_name = repository.rsplit("/", 1)[-1]
    base_url = f"https://github.com/{owner}/{repo_name}/releases/download/{tag}"
    homepage = f"https://github.com/{owner}/{repo_name}"

    assets = [f"rivet-{tag}-{target}.tar.gz" for target in TARGETS]
    shas = [sha_for(sums, asset) for asset in assets]

    # The attribution line named `dev/scripts/update_homebrew_formula.sh` while
    # that script still existed, so the port could be verified by diffing the two
    # formulas byte-for-byte. The .sh is now deleted, so the line names this
    # generator (as its own comment instructed).
    return f"""# typed: false
# frozen_string_literal: true

# Generated by python3 -m dev.pytools.homebrew_formula — do not edit checksums by hand.
# Re-run it after each release.

class Rivet < Formula
  desc "CLI to export PostgreSQL and MySQL to Parquet/CSV (local, S3, GCS)"
  homepage "{homepage}"
  version "{version}"
  license "MIT"

  on_macos do
    on_arm do
      url "{base_url}/{assets[0]}"
      sha256 "{shas[0]}"
    end
    on_intel do
      url "{base_url}/{assets[1]}"
      sha256 "{shas[1]}"
    end
  end

  on_linux do
    on_arm do
      url "{base_url}/{assets[2]}"
      sha256 "{shas[2]}"
    end
    on_intel do
      url "{base_url}/{assets[3]}"
      sha256 "{shas[3]}"
    end
  end

  def install
    # Both binaries shipped from 0.6.0 onwards.
    bin.install "rivet"
    bin.install "rivet-mcp" if File.exist?("rivet-mcp")
  end

  test do
    system "#{{bin}}/rivet", "--version"
  end
end
"""


# ── CLI ────────────────────────────────────────────────────────────────────────
class _Parser(argparse.ArgumentParser):
    """argparse with the bash's exit conventions: usage to stderr and exit 1 —
    for a bad or valueless flag (argparse's own default is 2) and for `-h`
    (argparse's is 0). A release script's exit codes are part of its contract."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise _Usage()

    def print_help(self, file=None) -> None:  # type: ignore[override]
        raise _Usage()


def _build_parser() -> _Parser:
    p = _Parser(prog=PROG, add_help=False, allow_abbrev=False)
    p.add_argument("--tag")
    p.add_argument("--sums-file", dest="sums_file")
    p.add_argument("-h", "--help", action="store_true", dest="help")
    return p


def main_cli(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        # `parse_known_args` so an unrecognised flag can be reported by NAME,
        # the way the bash's `*)` arm did, instead of argparse's own phrasing.
        args, extra = parser.parse_known_args(
            list(sys.argv[1:] if argv is None else argv)
        )
        if extra:
            print(f"Unknown option: {extra[0]}", file=sys.stderr)
            raise _Usage()
        if args.help:
            raise _Usage()
        # `--tag` / `--sums-file` with an empty value was `usage` in the bash.
        if args.tag is not None and not args.tag:
            raise _Usage()
        if args.sums_file is not None and not args.sums_file:
            raise _Usage()
    except _Usage:
        print(USAGE, file=sys.stderr)
        return 1

    if not CARGO_TOML.is_file():
        raise shell.Fail(f"error: Cargo.toml not found at {CARGO_TOML}")
    cargo = CARGO_TOML.read_text()

    version = _first_quoted("version", cargo)
    if not version:
        raise shell.Fail("error: could not parse version from Cargo.toml")

    tag_override = args.tag or ""
    if tag_override:
        tag = tag_override
        # `--tag v0.1.2` means the formula's version is `0.1.2`. A tag WITHOUT a
        # leading `v` leaves the version as Cargo.toml's — unlike release.py,
        # this script does not refuse such a tag (asset URLs use the tag as
        # given, so a bare `0.1.2` tag still resolves if the release is named so).
        if tag.startswith("v"):
            version = tag[1:]
    else:
        tag = f"v{version}"

    # Order matters: the pairing check must precede any download, because the
    # whole point is that a local sums file belongs to a specific release and
    # Cargo.toml's version may have already moved past it.
    if args.sums_file and not tag_override:
        raise shell.Fail(
            "error: --sums-file requires --tag (Cargo.toml version may not match the release assets)"
        )

    repository = resolve_repository(_first_quoted("repository", cargo))
    owner = repository.partition("/")[0]
    repo_name = repository.rsplit("/", 1)[-1]
    sums_url = f"https://github.com/{owner}/{repo_name}/releases/download/{tag}/SHA256SUMS.txt"

    if args.sums_file:
        sums_path = Path(args.sums_file)
        if not sums_path.is_file():
            raise shell.Fail(f"error: --sums-file not found: {args.sums_file}")
        sums_text = sums_path.read_text()
    else:
        sums_text = fetch_sums(sums_url, tag=tag, repository=repository)

    sums = parse_sums(sums_text)
    formula = render_formula(tag, sums, repository=repository, version=version)

    # `cat > "$OUT"` truncates before the generator can fail; a crash mid-write
    # left a half-formula that still looked like a file. Temp file + rename.
    shell.atomic_write(OUT, formula)

    print(f"Wrote {OUT}")
    print(
        "Next: copy to your tap repository as Formula/rivet.rb "
        "(see packaging/homebrew/README.md)."
    )
    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
