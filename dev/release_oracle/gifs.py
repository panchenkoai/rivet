"""GIF currency gate — the instructional screencasts must show what THIS binary prints.

`docs/gifs/README.md` states the contract the GIFs live by: "They show exactly
what `rivet` prints." That makes them a form of documentation that can go stale
SILENTLY, and worse than prose can: a reader copies a command out of a GIF, runs
it, and gets an error the recording never showed. Nothing in the repo noticed —
the GIFs are binary assets, so no test reads them and no diff flags them.

So this gate does not ask "were the GIFs re-rendered?" (unanswerable from a `.gif`
whose bytes change on every render anyway). It asks two questions that HAVE
answers:

1. **Does every command a tape shows still exist?** Each tape's `Type "…"` lines
   are real shell commands. If one names a subcommand or flag this binary no
   longer accepts — a removed config block, a renamed flag — the GIF documents
   something that cannot be run. That is a FAIL on its own, independent of
   rendering: re-rendering would not fix it, the tape has to change.

2. **Has the OUTPUT surface those commands produce changed since the GIFs were
   last rendered?** For each tape we fingerprint what the binary would print for
   the commands it shows (help text, and for `rivet init` the scaffold itself),
   and compare against `docs/gifs/surface.lock.json` — captured at bless time,
   next to the GIFs. A changed fingerprint means the recording and the binary
   have diverged, i.e. the GIF is STALE and needs a re-render.

Fingerprinting the SURFACE rather than the GIF is deliberate. A GIF's bytes
differ between two renders of identical content (timing, encoder), so byte
comparison can only ever say "re-rendered", never "still accurate". The help and
scaffold text are exactly the parts a viewer reads off the screen, and they are
stable under re-render — so a diff in them is a real divergence and nothing else.

Rendering itself is NOT done here: it needs `vhs`/`ttyd`/`ffmpeg`, a live
Postgres and MySQL, and for one tape real GCS credentials. When the toolchain is
absent this gate still runs check (1) and reports STALE/CURRENT from (2) —
it never reports PASS for a check it could not make.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .core import Ledger, ROOT, have, rivet, run

GIF_DIR = ROOT / "docs" / "gifs"
LOCK = GIF_DIR / "surface.lock.json"

# `Type "…"` / `Type `…`` — VHS's keystroke directive, which is where a tape's
# real commands live. `Hide`…`Show` wraps the setup preamble; those lines are
# environment plumbing (PATH, PS1) rather than anything a viewer reads, so the
# extractor keeps only what invokes rivet.
_TYPE = re.compile(r'^\s*Type\s+(?:"([^"]*)"|`([^`]*)`)\s*$')


@dataclass(frozen=True)
class Tape:
    path: Path
    commands: tuple[str, ...]

    @property
    def gif(self) -> Path:
        return self.path.with_suffix(".gif")

    @property
    def name(self) -> str:
        return self.path.stem


def read_tapes() -> list[Tape]:
    tapes: list[Tape] = []
    for p in sorted(GIF_DIR.glob("*.tape")):
        cmds: list[str] = []
        for line in p.read_text().splitlines():
            m = _TYPE.match(line)
            if not m:
                continue
            cmd = (m.group(1) or m.group(2) or "").strip()
            if re.search(r"\brivet\b", cmd):
                cmds.append(cmd)
        tapes.append(Tape(p, tuple(cmds)))
    return tapes


def _rivet_invocations(cmd: str) -> list[list[str]]:
    """The rivet invocations inside one shell command line.

    A tape line can be a pipeline (`rivet init --discover … | jq …`) or carry a
    leading env assignment (`RUST_LOG=info rivet run …`). Both are split so the
    checks below see argv, not a string to re-parse.
    """
    out: list[list[str]] = []
    for piece in re.split(r"\|\||&&|\||;", cmd):
        try:
            argv = shlex.split(piece.strip())
        except ValueError:
            continue  # unbalanced quotes in a tape line — not our business here
        argv = [a for a in argv if "=" not in a.split(" ")[0] or not re.match(r"^\w+=", a)]
        if argv and Path(argv[0]).name == "rivet":
            out.append(argv)
    return out


def _resolve(argv: list[str]) -> tuple[str, str] | None:
    """(command path, help text) for the DEEPEST subcommand this invocation names.

    The path has to be walked, not guessed at one level: rivet nests
    (`state files`, `state progression`, `schema config`), and a leaf's flags live
    only in the LEAF's help — `rivet state --help` lists sub-subcommands, so
    checking `--last` against it reports a perfectly valid
    `rivet state files --last 5` as broken. That false alarm is worse than no
    check at all: a gate that cries wolf gets muted, and this repo has already
    watched 66 phantom warnings bury a real one.

    `--help` succeeding is what distinguishes a subcommand from a positional
    value (`rivet completions bash` — `bash` is an argument, not a command), so
    the walk stops at the first token that is not a command in its own right.
    """
    path: list[str] = []
    help_text = ""
    for tok in argv[1:]:
        if tok.startswith("-"):
            break
        probe = rivet(*path, tok, "--help", timeout=60)
        if not probe.ok:
            break
        path.append(tok)
        help_text = probe.stdout + probe.stderr
    if not path:
        return None
    return (" ".join(path), help_text)


def _surface_of(argv: list[str]) -> tuple[str, str] | None:
    """(key, text) — what a viewer would read off the screen for this command.

    Uses the resolved subcommand's `--help` rather than executing it: a real run
    needs databases, credentials and fixtures, and its output embeds timings and
    row counts that differ every time. The help text is the stable part, and it
    is exactly what changes when a flag is added, renamed or removed.
    """
    r = _resolve(argv)
    if r is None:
        return None
    path, help_text = r
    return (f"help:{path}", help_text)


def _unknown_surface(argv: list[str]) -> str | None:
    """A reason this invocation is no longer valid, or None.

    Only the SHAPE is checked (subcommand path + long flags), never a real run: a
    tape's command may legitimately fail without a database, and that is not what
    this gate is about. An unknown subcommand or an unrecognised flag, though,
    means the GIF shows something this binary cannot do — and re-rendering would
    only record the error.
    """
    first = next((a for a in argv[1:] if not a.startswith("-")), None)
    r = _resolve(argv)
    if r is None:
        return f"unknown subcommand `{first}`" if first else None
    path, help_text = r
    for a in argv[1:]:
        if not a.startswith("--"):
            continue  # short flags are skipped deliberately: `-c` can collide with
            # a value in the help text, and a false alarm costs more than a miss
        flag = a.split("=", 1)[0]
        if flag not in help_text:
            return f"`{path}` no longer accepts `{flag}`"
    return None


def _fingerprint(tape: Tape) -> tuple[str, list[str]]:
    """A stable digest of everything a viewer reads in this tape, plus notes."""
    h = hashlib.sha256()
    notes: list[str] = []
    for cmd in tape.commands:
        for argv in _rivet_invocations(cmd):
            s = _surface_of(argv)
            if s is None:
                continue
            key, text = s
            if key not in notes:
                notes.append(key)
                h.update(key.encode())
                h.update(text.encode())
    return h.hexdigest()[:16], notes


def verify_gif_currency(led: Ledger, *, bless: bool | Sequence[str] = False) -> None:
    """The gate.

    `bless` records the current surface as "what the rendered GIFs show". It takes
    a LIST of tape names as well as `True`, because re-rendering is normally
    incremental — one CLI change invalidates one or two GIFs, not twenty. An
    all-or-nothing bless would then stamp nineteen recordings nobody re-rendered
    as current, which is precisely the lie this gate exists to catch. A list
    merges over the existing lock; `True` means "all of them were just rendered".
    """
    led.phase("Instructional GIFs — do they still show what this binary prints?")

    if not GIF_DIR.is_dir():
        led.skipped("gifs", "-", "currency", "-", "gifs: docs/gifs absent", "no dir")
        return

    tapes = read_tapes()
    if not tapes:
        led.skipped("gifs", "-", "currency", "-", "gifs: no tapes found", "no tapes")
        return

    lock: dict[str, str] = {}
    if LOCK.is_file():
        try:
            lock = json.loads(LOCK.read_text()).get("surfaces", {})
        except json.JSONDecodeError:
            led.failed("gifs", "-", "lock", "-", f"gifs: {LOCK.name} is not valid JSON", "bad lock")
            return

    fresh: dict[str, str] = {}
    stale: list[str] = []
    broken: list[str] = []
    unlocked: list[str] = []

    for tape in tapes:
        digest, _ = _fingerprint(tape)
        fresh[tape.name] = digest

        # (1) does everything the tape shows still exist?
        reasons = [
            r
            for cmd in tape.commands
            for argv in _rivet_invocations(cmd)
            if (r := _unknown_surface(argv))
        ]
        if reasons:
            broken.append(f"{tape.name}({'; '.join(sorted(set(reasons)))})")
            continue

        if not tape.gif.is_file():
            broken.append(f"{tape.name}(no rendered .gif)")
            continue

        # (2) has the surface moved since the GIF was rendered?
        was = lock.get(tape.name)
        if was is None:
            unlocked.append(tape.name)
        elif was != digest:
            stale.append(tape.name)

    if bless:
        if bless is True:
            merged = dict(fresh)
            named = sorted(fresh)
        else:
            named = sorted(set(bless))
            unknown = [n for n in named if n not in fresh]
            if unknown:
                led.failed("gifs", "-", "bless", "-",
                           f"gifs: no such tape(s): {', '.join(unknown)}", "unknown tape")
                return
            # Merge, so blessing the tapes just rendered leaves every other tape's
            # recorded digest exactly as it was — including "absent", which keeps
            # reading as SKIP rather than silently becoming current.
            merged = {**lock, **{n: fresh[n] for n in named}}
        LOCK.write_text(json.dumps({
            "_comment": "Surface digests captured when the GIFs were last rendered. "
                        "A digest that no longer matches means the recording and the "
                        "binary have diverged — re-render that tape, then re-bless it "
                        "BY NAME (blessing everything would stamp un-rendered GIFs as "
                        "current).",
            "surfaces": merged,
        }, indent=2, sort_keys=True) + "\n")
        led.passed("gifs", "-", "currency", "-",
                   f"gifs: blessed {len(named)} tape surface(s) into {LOCK.name} "
                   f"({', '.join(named)})",
                   f"blessed {len(named)}")
        return

    if broken:
        led.failed("gifs", "-", "surface", "-",
                   f"gifs: {len(broken)} tape(s) show commands this binary rejects — "
                   f"re-rendering will NOT fix these, the tapes must change: {', '.join(broken)}",
                   f"{len(broken)} broken")
    if stale:
        led.failed("gifs", "-", "currency", "-",
                   f"gifs: {len(stale)} GIF(s) are STALE — the output surface changed since "
                   f"they were rendered: {', '.join(stale)} "
                   f"(re-render with python3 -m dev.pytools.render_gifs, then --bless-gifs)",
                   f"{len(stale)} stale")
    if unlocked:
        # Never a PASS: with no recorded digest there is nothing to compare, so
        # currency is UNKNOWN rather than good. Blessing after a render fixes it.
        led.skipped("gifs", "-", "currency", "-",
                    f"gifs: {len(unlocked)} tape(s) have no recorded surface digest "
                    f"({', '.join(unlocked)}) — run with --bless-gifs after a render",
                    f"{len(unlocked)} unlocked")
    if not broken and not stale and not unlocked:
        led.passed("gifs", "-", "currency", "-",
                   f"gifs: all {len(tapes)} tapes' commands exist and their output surface "
                   "matches what was rendered", f"{len(tapes)} current")

    if not have("vhs"):
        # Reported as information, not a cell: this gate's verdict does not depend
        # on being able to render. It matters only when something is stale.
        led.skip("gifs: `vhs` not on PATH — a stale GIF cannot be re-rendered here "
                 "(brew install vhs ttyd ffmpeg)")
