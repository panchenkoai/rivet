#!/usr/bin/env python3
"""Regenerate rivet's instructional GIFs with VHS — the Python port of
`docs/gifs/render.sh`.

Prerequisites:
  * `vhs`, `ttyd`, `ffmpeg` on PATH (Homebrew: `brew install vhs`).
  * Docker Desktop up with the repo stack: `docker compose up -d postgres mysql`.
  * `target/release/rivet` + `target/release/seed` (built here if missing).

Each scenario is four steps, in this order:
  1. audit the tape's `rivet` commands against the BINARY (see `audit_tape`),
  2. prepare an ephemeral `/tmp/rivet-gif-<name>` workdir + its DB fixtures,
  3. run `vhs <name>.tape` from inside that workdir, producing `<name>.gif`,
  4. drop the fixtures — always, including when the render blew up.

Safe to run repeatedly. Ephemeral fixtures live in schema `rivet_gif` (tables
addressed as `rivet_gif.*`), and teardown drops that schema and nothing else, so
a teardown never reaches user data.

One exception, carried over from the bash script and worth knowing before you
run it: the `basic` / `plan-apply` fixture `TRUNCATE`s `public.orders` in the dev
stack and reseeds it with 250K synthetic rows. That table belongs to the local
docker stack, not to a user — but it is outside the `rivet_gif` sandbox, so point
this at a dev database only.

Deliberate departures from the bash original, each because the bash shape had a
failure mode we can now see:

* **The tape audit is new** (step 1). A tape that passes a flag the CLI has
  since dropped still records happily — vhs types the line, rivet prints
  `error: unexpected argument`, and the GIF ships a broken command as
  documentation. `docs/gifs/cdc.tape` did exactly this with `--until-current`
  (bounded draining is now the DEFAULT; `--stream` is the opt-in for
  continuous). The audit makes that a loud, specific failure BEFORE any seeding.
* **Teardown runs on every exit path.** Bash relied on `set -e`, so a failing
  `vhs` — or a fixture that died halfway — aborted the script before its
  teardown line, leaving the `rivet_gif` schema (35M rows for `plan-campaign`)
  or the `rivet_noperm` MySQL user behind for the next scenario to trip over.
* **Shared fixtures write into the CURRENT scenario's workdir.** Bash hard-coded
  `/tmp/rivet-gif-reconcile-repair` inside `fixture_chunked_setup`, so the four
  scenarios that reuse it created a stray directory nobody ever cleaned. Only
  `reconcile-repair.tape` reads `chunked.yaml`, and for that scenario the path
  is unchanged.
* **Silent-fixture guards.** The MySQL checkpoint seed used
  `SHOW MASTER STATUS | awk`, which writes an EMPTY `cdc.ckpt` when the position
  is unreadable — rivet then re-anchors to "now" and the GIF records a drain of
  nothing. It is now a parse with a real error, behind the same idempotent
  REPLICATION grant the bash fixture grew (the compose `rivet` user holds
  nothing global, so both the position read and the binlog dump were denied).
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
import shell  # noqa: E402
from shell import Fail, ROOT, bad, log, ok, run, warn  # noqa: E402

TAG = "gifs"

HERE = ROOT / "docs" / "gifs"
BIN = ROOT / "target" / "release" / "rivet"
SEED = ROOT / "target" / "release" / "seed"
BIN_DIR = ROOT / "target" / "release"
COMPOSE_FILE = ROOT / "docker-compose.yaml"

PG_HOST = os.environ.get("PGHOST") or "localhost"
PG_PORT = os.environ.get("PGPORT") or "5432"
PG_USER = os.environ.get("PGUSER") or "rivet"
PG_PASSWORD = os.environ.get("PGPASSWORD") or "rivet"
PG_DB = os.environ.get("PGDATABASE") or "rivet"
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

GCS_DEMO_BUCKET = os.environ.get("GCS_DEMO_BUCKET") or "rivet_data_test"
GCS_DEMO_PREFIX = os.environ.get("GCS_DEMO_PREFIX") or "rivet-gif-demo/"

LIBPQ_PSQL = Path("/opt/homebrew/opt/libpq/bin/psql")


# ── source access ──────────────────────────────────────────────────────────────
def psql_bin() -> str:
    """The psql the TAPES get on their PATH — `""` when there is none on the host
    (the tapes that interpolate `$PSQL_BIN` degrade the same way bash's empty
    `psql_bin` made them)."""
    found = shutil.which("psql")
    if found:
        return found
    if os.access(LIBPQ_PSQL, os.X_OK):
        return str(LIBPQ_PSQL)
    return ""


def gcloud_bin() -> str:
    return shutil.which("gcloud") or ""


def _psql_argv() -> list[str]:
    """psql on PATH → the Homebrew libpq keg → inside the compose container.

    The container arm drops `-h/-p` (it connects container-local) and passes
    `PGPASSWORD` EXPLICITLY: `docker compose exec` does not inherit the host
    environment, so bash's exported `PGPASSWORD` never reached the fallback and
    a host without psql got a password prompt instead of a fixture.
    """
    conn = ["-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", PG_DB, "-v", "ON_ERROR_STOP=1"]
    found = shutil.which("psql")
    if found:
        return [found, *conn]
    if os.access(LIBPQ_PSQL, os.X_OK):
        return [str(LIBPQ_PSQL), *conn]
    return [
        "docker", "compose", "-f", str(COMPOSE_FILE),
        "exec", "-T", "-e", f"PGPASSWORD={PG_PASSWORD}", "postgres",
        "psql", "-U", PG_USER, "-d", PG_DB, "-v", "ON_ERROR_STOP=1",
    ]


def psql(sql: str, *, what: str) -> shell.Proc:
    """Run a SQL script on stdin (bash's `psql_ <<'SQL'` heredoc)."""
    return run(
        _psql_argv(), stdin=sql, env={"PGPASSWORD": PG_PASSWORD}, timeout=None
    ).check(what)


def psql_c(sql: str, *, what: str) -> shell.Proc:
    return run(
        [*_psql_argv(), "-c", sql], env={"PGPASSWORD": PG_PASSWORD}, timeout=None
    ).check(what)


def mysql(*args: str, root: bool = False, sql: str | None = None) -> shell.Proc:
    """The CDC scenarios need the compose stack's MySQL specifically — the binlog
    position they checkpoint is that server's. `root=True` for the grant work,
    since `rivet` itself cannot create users."""
    user = "root" if root else "rivet"
    extra = ["-e", sql] if sql is not None else []
    return shell.compose(
        "exec", "-T", "mysql", "mysql", f"-u{user}", "-privet", "rivet", *args, *extra,
        file=COMPOSE_FILE, timeout=None,
    )


# ── prerequisites ──────────────────────────────────────────────────────────────
def ensure_prereqs() -> None:
    shell.require("vhs", hint="brew install vhs")
    shell.require("ttyd", hint="brew install ttyd")
    shell.require("ffmpeg", hint="brew install ffmpeg")

    if not (os.access(BIN, os.X_OK) and os.access(SEED, os.X_OK)):
        log("building release binaries ...", tag=TAG)
        shell.stream(
            ["cargo", "build", "--release", "--bin", "rivet", "--bin", "seed",
             "--features", "dev-seed"],
            cwd=ROOT, timeout=None,
        ).check("cargo build --release")

    probe = run([*_psql_argv(), "-c", "SELECT 1"], env={"PGPASSWORD": PG_PASSWORD}, timeout=60)
    if not probe.ok:
        raise Fail(
            f"postgres not reachable at {DATABASE_URL}",
            hint="docker compose up -d postgres",
        )


# ── tape audit: check every tape's rivet commands against the binary ───────────
#
# A GIF is documentation, and a recording cannot notice that the CLI moved under
# it. So before rendering we parse the tape's `Type` lines, pull out the `rivet`
# invocations, and ask THIS binary whether it would still accept them.
#
# Two stages, on purpose:
#   * a static pass over `--help` names suspicious flags (cheap, no execution);
#   * the BINARY delivers the verdict — argv + `--help`, which clap rejects at
#     the offending token before it can reach a run. A suspect the binary
#     accepts is a gap in our help scraping, so it downgrades to a warning; only
#     a real clap parse error fails the render.
# That ordering means the gate cannot invent a failure, and cannot execute the
# tape's command as a side effect of checking it.

_TYPE_RE = re.compile(r"^\s*Type(?:@\S+)?\s+(.*\S)\s*$")
_FLAG_RE = re.compile(r"--[A-Za-z0-9][A-Za-z0-9-]*|(?<![\w-])-[A-Za-z0-9](?![\w-])")
_ENV_ASSIGN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_REDIRECT_RE = re.compile(r"^\d*[<>]|^&>")
_CLAP_REJECTIONS = (
    "unexpected argument",
    "unrecognized subcommand",
    "invalid subcommand",
    "wasn't expected",
    "unexpected value",
)


def _tape_typed_lines(tape: Path) -> list[tuple[int, str]]:
    """`(lineno, payload)` for every `Type` directive, outer quoting removed.

    Tapes quote with `"`, `'` or a backtick; the backtick form is how they get a
    literal `$VAR` or an embedded `'…'` past vhs's parser.
    """
    out: list[tuple[int, str]] = []
    for lineno, raw in enumerate(tape.read_text().splitlines(), start=1):
        m = _TYPE_RE.match(raw)
        if not m:
            continue
        payload = m.group(1)
        if len(payload) >= 2 and payload[0] in "\"'`" and payload[-1] == payload[0]:
            payload = payload[1:-1]
        out.append((lineno, payload))
    return out


def _split_pipeline(line: str) -> list[str]:
    """Split a shell line into command segments on `|`, `;` and `&&`, ignoring
    separators inside quotes — several tapes pipe rivet into
    `grep -E 'a|b|c'`, and a quote-blind split would mangle the pattern (and,
    worse, could hide a rivet command inside the wreckage). A lone `&` stays put
    so `2>&1` survives as one token."""
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i = 0
    while i < len(line):
        ch = line[i]
        if quote is not None:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in "\"'`":
            quote = ch
            buf.append(ch)
        elif ch == "\\" and i + 1 < len(line):
            buf.append(ch)
            buf.append(line[i + 1])
            i += 1
        elif ch == "|" or ch == ";" or (ch == "&" and line[i : i + 2] == "&&"):
            parts.append("".join(buf))
            buf = []
            while i + 1 < len(line) and line[i + 1] in "|;&":
                i += 1
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


@lru_cache(maxsize=None)
def _help_text(path: tuple[str, ...]) -> str:
    """`rivet <path> --help`, checked. An unusable binary must not degrade into an
    EMPTY flag set — that would make every flag a "suspect" the binary then can't
    adjudicate, i.e. an audit that silently stops auditing."""
    p = run([str(BIN), *path, "--help"], timeout=60)
    if not p.ok:
        raise Fail(
            f"`{' '.join(('rivet', *path))} --help` failed — cannot audit the tapes "
            f"against {BIN}",
            hint=(p.stderr or p.stdout).strip().splitlines()[-1:][0]
            if (p.stderr or p.stdout).strip()
            else f"exit {p.returncode}",
        )
    return p.out


@lru_cache(maxsize=None)
def _known_flags(path: tuple[str, ...]) -> frozenset[str]:
    """Flag names clap documents for `rivet <path>`.

    Harvested only from option-DEFINITION lines — indented at most 8 columns and
    starting with a dash. Description text sits at 10+, so prose that happens to
    mention a flag cannot widen the set (which would be a silent false negative,
    exactly what let `--until-current` survive in the tape).
    """
    flags: set[str] = set()
    for raw in _help_text(path).splitlines():
        stripped = raw.strip()
        if not stripped.startswith("-"):
            continue
        if len(raw) - len(raw.lstrip()) > 8:
            continue
        spec = re.split(r"\s{2,}", stripped, maxsplit=1)[0]
        flags.update(_FLAG_RE.findall(spec))
    return frozenset(flags)


@lru_cache(maxsize=None)
def _subcommands(path: tuple[str, ...]) -> frozenset[str]:
    """Names in the `Commands:` block of `rivet <path> --help`, so a nested path
    like `rivet state progression` resolves to the right help page."""
    names: set[str] = set()
    in_commands = False
    for raw in _help_text(path).splitlines():
        if not raw.strip():
            continue
        if not raw.startswith(" "):
            in_commands = raw.strip() == "Commands:"
            continue
        if not in_commands:
            continue
        indent = len(raw) - len(raw.lstrip())
        m = re.match(r"^(\S+)(?:\s{2,}|$)", raw.strip())
        if indent <= 4 and m and m.group(1) != "help":
            names.add(m.group(1))
    return frozenset(names)


@dataclass(frozen=True)
class RivetCall:
    lineno: int
    command: str
    path: tuple[str, ...]
    argv: tuple[str, ...]


def _rivet_calls(tape: Path) -> list[RivetCall]:
    calls: list[RivetCall] = []
    for lineno, payload in _tape_typed_lines(tape):
        for segment in _split_pipeline(payload):
            try:
                toks = shlex.split(segment)
            except ValueError:
                # An unbalanced quote here means the segment is a fragment of a
                # quoted argument (a grep pattern), never a command of its own.
                continue
            toks = [t for t in toks if not _REDIRECT_RE.match(t)]
            while toks and _ENV_ASSIGN_RE.match(toks[0]):
                toks.pop(0)
            if not toks or Path(toks[0]).name != "rivet":
                continue
            path: list[str] = []
            rest = toks[1:]
            while rest and not rest[0].startswith("-") and rest[0] in _subcommands(tuple(path)):
                path.append(rest.pop(0))
            calls.append(RivetCall(lineno, segment, tuple(path), tuple(toks[1:])))
    return calls


def _suspect_flags(call: RivetCall) -> list[str]:
    allowed = _known_flags(()) | _known_flags(call.path)
    suspects: list[str] = []
    for tok in call.argv:
        if not tok.startswith("-") or tok in ("-", "--"):
            continue
        name = tok.split("=", 1)[0]
        if name.startswith("--"):
            candidates = [name]
        else:
            # `-Nb`-style clusters: every letter is its own flag.
            candidates = [f"-{c}" for c in name[1:]]
        for c in candidates:
            if c not in allowed and c not in suspects:
                suspects.append(c)
    return suspects


def _binary_rejection(call: RivetCall) -> str | None:
    """Ask the binary. `--help` is appended so clap stops at the parse stage: an
    unknown flag errors before any work happens, and a clean command just prints
    help. Only clap's own parse-error wording counts as a rejection — a missing
    required argument or a bad config path is not the tape being stale."""
    p = run([str(BIN), *call.argv, "--help"], timeout=60)
    if p.ok:
        return None
    text = p.out
    if not any(marker in text for marker in _CLAP_REJECTIONS):
        return None
    for line in text.splitlines():
        if line.strip().startswith("error:"):
            return line.strip()
    return text.strip().splitlines()[0] if text.strip() else "rejected by the CLI"


def audit_tape(tape: Path) -> None:
    """Fail loudly and specifically when a tape types a `rivet` command this
    binary no longer accepts — rendering it would only produce a GIF of an error
    message, published as documentation."""
    for call in _rivet_calls(tape):
        suspects = _suspect_flags(call)
        if not suspects:
            continue
        rejection = _binary_rejection(call)
        subcommand = " ".join(("rivet", *call.path))
        if rejection is None:
            warn(
                f"{tape.name}:{call.lineno} — {', '.join(suspects)} is not in "
                f"`{subcommand} --help`, but the binary accepts the command; "
                "treating it as a help-scraping gap, not a stale tape"
            )
            continue
        raise Fail(
            f"{tape.name}:{call.lineno} types a command this binary REJECTS "
            f"({', '.join(suspects)}): {rejection}\n"
            f"      command: {call.command}",
            hint=(
                f"the CLI moved under the tape — check `{subcommand} --help` and "
                "update the tape; rendering it would publish a GIF of an error "
                "message. (Bounded CDC draining is the default now; `--stream` "
                "is the opt-in for continuous.)"
            ),
        )


# ── fixtures ───────────────────────────────────────────────────────────────────
# Every fixture takes the scenario's own workdir, so a config it drops for the
# tape lands next to the tape that reads it.

def _noop(work: Path) -> None:
    return None


def fixture_basic(work: Path) -> None:
    # `public.orders` at ~250K rows sits in the band where `rivet init` picks
    # chunked mode AND `rivet run` still finishes inside the recording window —
    # fewer rows and init scaffolds a plain full export, more and the run
    # outlasts the GIF.
    psql(
        """
TRUNCATE orders RESTART IDENTITY CASCADE;
INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT (g % 500) + 1,
       'sku-' || g,
       (g % 5) + 1,
       (g % 100) + 0.99,
       'pending',
       'gif-fixture',
       NOW() - (g || ' minutes')::interval,
       NOW() - (g || ' minutes')::interval
FROM generate_series(1, 250000) AS g;
ANALYZE orders;
""",
        what="seed public.orders (250K rows)",
    )


def fixture_chunked(work: Path) -> None:
    """10k deterministic rows in `rivet_gif.events` — the shared base fixture."""
    psql(
        """
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.events (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT        NOT NULL,
    event_type  TEXT          NOT NULL,
    payload     TEXT          NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

INSERT INTO rivet_gif.events (id, user_id, event_type, payload)
SELECT
    g,
    (g % 500) + 1,
    CASE (g % 3) WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' ELSE 'purchase' END,
    'payload-' || g
FROM generate_series(1, 10000) AS g;

CREATE INDEX ON rivet_gif.events (created_at);
""",
        what="seed rivet_gif.events (10K rows)",
    )

    # The config goes on disk rather than into the tape: vhs `Type` cannot carry
    # a multi-line heredoc cleanly, so the tape just references the file.
    shell.atomic_write(
        work / "chunked.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 2500
    chunk_checkpoint: true
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_chunked_teardown(work: Path) -> None:
    psql_c("DROP SCHEMA IF EXISTS rivet_gif CASCADE;", what="drop schema rivet_gif")


def fixture_check(work: Path) -> None:
    # `rivet check` reads a config that must already exist, so scaffold one
    # quietly here instead of spending recording time on `init`.
    run(
        [str(BIN), "init", "--source-env", "DATABASE_URL", "--table", "orders",
         "-o", "orders.yaml"],
        cwd=work, env={"DATABASE_URL": DATABASE_URL}, timeout=None,
    ).check("rivet init (check-verdict fixture)")


def fixture_inspect(work: Path) -> None:
    # The inspect tape shows `state show` / `metrics` / `state files` /
    # `state progression`, so it needs a completed run to read. It runs the
    # CHUNKED shape on purpose: an incremental-only run leaves progression
    # empty, and an empty panel is the one thing the GIF must not show.
    fixture_chunked(work)

    shell.atomic_write(
        work / "orders_incremental.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 2500
    chunk_checkpoint: true
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )

    env = {"DATABASE_URL": DATABASE_URL}
    run([str(BIN), "run", "--config", "orders_incremental.yaml", "--validate"],
        cwd=work, env=env, timeout=None).check("rivet run (inspect fixture)")
    # Reconcile only enriches the panels; a mismatch here must not block the
    # render (bash's `|| true`).
    rec = run([str(BIN), "reconcile", "--config", "orders_incremental.yaml",
               "--export", "events"], cwd=work, env=env, timeout=None)
    if not rec.ok:
        warn("inspect fixture: reconcile returned non-zero (tolerated)")


def fixture_chunked_progress(work: Path) -> None:
    # 50k rows so the chunk counter has enough steps to visibly advance.
    psql(
        """
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;
CREATE TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO rivet_gif.events (id, user_id, event_type, payload)
SELECT g, (g % 500) + 1,
       CASE (g % 3) WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' ELSE 'purchase' END,
       repeat('payload-', 8) || g
FROM generate_series(1, 50000) AS g;
""",
        what="seed rivet_gif.events (50K rows)",
    )

    shell.atomic_write(
        work / "chunked-progress.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    # Small batches + a per-batch throttle so the progress bar ticks at a
    # readable pace. Demo only — production uses `balanced` / `fast`.
    batch_size: 1000
    throttle_ms: 80
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_parallel_cards(work: Path) -> None:
    # Four narrow tables of different sizes so the parent-side card UI has four
    # cards advancing at visibly different rates.
    psql(
        """
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.orders (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.orders (id, payload)
SELECT g, repeat('o', 32) FROM generate_series(1, 60000) AS g;

CREATE TABLE rivet_gif.users (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.users (id, payload)
SELECT g, repeat('u', 32) FROM generate_series(1, 30000) AS g;

CREATE TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events (id, payload)
SELECT g, repeat('e', 32) FROM generate_series(1, 80000) AS g;

CREATE TABLE rivet_gif.sessions (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.sessions (id, payload)
SELECT g, repeat('s', 32) FROM generate_series(1, 40000) AS g;
""",
        what="seed rivet_gif.{orders,users,events,sessions}",
    )

    shell.atomic_write(
        work / "parallel-cards.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    # Small batches + an exaggerated throttle so every card has visibly ticking
    # progress during the recording window. Demo only — production runs use
    # `balanced` / `fast`.
    batch_size: 1000
    throttle_ms: 250
exports:
  - name: orders
    query: "SELECT id, payload FROM rivet_gif.orders"
    mode: chunked
    chunk_column: id
    chunk_size: 10000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: users
    query: "SELECT id, payload FROM rivet_gif.users"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: events
    query: "SELECT id, payload FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 10000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: sessions
    query: "SELECT id, payload FROM rivet_gif.sessions"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_incremental(work: Path) -> None:
    """The 10k-row events fixture plus an incremental config over it."""
    fixture_chunked(work)
    shell.atomic_write(
        work / "incremental.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: incremental
    cursor_column: id
    format: parquet
    skip_empty: true
    destination:
      type: local
      path: ./output
""",
    )


def fixture_coalesce(work: Path) -> None:
    # Composite-cursor demo (ADR-0007 CC1–CC6): ~35% of rows have a NULL
    # `updated_at` (deterministic on `id % 100 < 35`), which is what makes
    # `incremental_cursor_mode: coalesce` the only setting that tracks
    # progression without dropping the NULL-only rows.
    psql(
        """
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.orders (
    id          BIGINT PRIMARY KEY,
    product     TEXT          NOT NULL,
    quantity    INT           NOT NULL,
    price       NUMERIC(10,2) NOT NULL,
    updated_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- 200 rows; ~35% with updated_at NULL (deterministic via id % 100 < 35).
INSERT INTO rivet_gif.orders (id, product, quantity, price, updated_at, created_at)
SELECT g,
       'sku-' || g,
       (g % 10) + 1,
       (g % 100) + 0.99,
       CASE WHEN g % 100 < 35 THEN NULL ELSE NOW() - (g || ' minutes')::interval END,
       NOW() - ((g + 200) || ' minutes')::interval
FROM generate_series(1, 200) AS g;
""",
        what="seed rivet_gif.orders (200 rows, ~35% NULL updated_at)",
    )

    shell.atomic_write(
        work / "orders.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id, product, quantity, price, updated_at, created_at FROM rivet_gif.orders"
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: created_at
    incremental_cursor_mode: coalesce
    format: parquet
    skip_empty: true
    destination:
      type: local
      path: ./output
""",
    )


def fixture_discover(work: Path) -> None:
    # The discovery tape pretty-prints the artifact with jq; the already-seeded
    # `public.*` schema is the subject, so there is nothing to seed.
    shell.require("jq", hint="brew install jq")


def fixture_campaign(work: Path) -> None:
    # Two tables past 10M rows so `classify_cost` returns High / VeryHigh; the
    # shared `source_group: replica_main` then trips
    # `shared_source_heavy_conflict` and the campaign-level warning the tape is
    # about. Narrow rows + generate_series + UNLOGGED keeps the seed ~20–40s.
    psql(
        """
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE UNLOGGED TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events (id, payload)
SELECT g, 'p' FROM generate_series(1, 20000000) AS g;

CREATE UNLOGGED TABLE rivet_gif.events_archive (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events_archive (id, payload)
SELECT g, 'a' FROM generate_series(1, 15000000) AS g;

ANALYZE rivet_gif.events;
ANALYZE rivet_gif.events_archive;
""",
        what="seed rivet_gif.events + events_archive (~35M rows)",
    )

    shell.atomic_write(
        work / "campaign.yaml",
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, payload FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 500000
    source_group: replica_main
    format: parquet
    destination:
      type: local
      path: ./output

  - name: events_archive
    query: "SELECT id, payload FROM rivet_gif.events_archive"
    mode: chunked
    chunk_column: id
    chunk_size: 500000
    source_group: replica_main
    format: parquet
    destination:
      type: local
      path: ./output

  - name: users_dim
    query: "SELECT id, email FROM users"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_pool_detect(work: Path) -> None:
    # Opt-in scenario: needs the `pool` compose profile so pgBouncer (6432) and
    # ProxySQL (6033) answer —
    #   docker compose --profile pool up -d pgbouncer proxysql
    # Four probe configs: direct PG, pgBouncer, direct MySQL, ProxySQL. Each
    # `SELECT 1` export exits in well under a second, so the connect-time
    # warning is the only thing on screen.
    probes = {
        "direct-pg.yaml": ("postgres", "postgresql://rivet:rivet@localhost:5432/rivet"),
        "bouncer-pg.yaml": ("postgres", "postgresql://rivet:rivet@localhost:6432/rivet"),
        "direct-mysql.yaml": ("mysql", "mysql://rivet:rivet@127.0.0.1:3306/rivet"),
        "proxysql.yaml": ("mysql", "mysql://rivet:rivet@127.0.0.1:6033/rivet"),
    }
    for filename, (kind, url) in probes.items():
        shell.atomic_write(
            work / filename,
            f"""source:
  type: {kind}
  url: "{url}"
exports:
  - name: probe
    query: "SELECT 1 AS n"
    mode: full
    format: parquet
    destination: {{type: local, path: ./output}}
""",
        )


def fixture_gcs(work: Path) -> None:
    # Opt-in scenario: writes to a REAL bucket using Application Default
    # Credentials, which is the whole point (no credential file on disk).
    gcloud = shutil.which("gcloud")
    if not gcloud:
        raise Fail(
            "`gcloud` not found on PATH",
            hint="install google-cloud-sdk, then `gcloud auth application-default login`",
        )
    adc = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    if not adc.is_file():
        raise Fail(
            f"Application Default Credentials missing at {adc}",
            hint="gcloud auth application-default login",
        )

    shell.atomic_write(
        work / "gcs.yaml",
        f"""source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: users_sample
    query: "SELECT id, email, created_at FROM users LIMIT 100"
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: {GCS_DEMO_BUCKET}
      prefix: {GCS_DEMO_PREFIX}
""",
    )

    # Wipe leftovers from an earlier take so the closing `gcloud storage ls`
    # shows only what THIS recording wrote. An empty prefix makes `rm` fail;
    # that is the normal case, not an error.
    _gcs_wipe(gcloud)


def fixture_gcs_teardown(work: Path) -> None:
    gcloud = shutil.which("gcloud")
    if gcloud:
        _gcs_wipe(gcloud)


def _gcs_wipe(gcloud: str) -> None:
    run([gcloud, "storage", "rm", "--quiet", "--recursive",
         f"gs://{GCS_DEMO_BUCKET}/{GCS_DEMO_PREFIX}"], timeout=300)


# ── error / recovery fixtures ──────────────────────────────────────────────────
# Each drops a tiny `rivet.yaml` that triggers one of rivet's actionable failure
# messages. No seeding: the missing-table probe only needs Postgres reachable
# (DATABASE_URL is in the tape env), the parse error needs no DB at all, and the
# connection tape overrides DATABASE_URL to a dead port itself.

def _write_rivet_yaml(work: Path, body: str) -> None:
    shell.atomic_write(work / "rivet.yaml", body)


def fixture_error_missing_table(work: Path) -> None:
    _write_rivet_yaml(
        work,
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id, total FROM ordrs"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_error_config_typo(work: Path) -> None:
    # `export:` instead of `exports:` — the unknown-key suggestion path.
    _write_rivet_yaml(
        work,
        """source:
  type: postgres
  url_env: DATABASE_URL
export:
  - name: orders
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


def fixture_error_connection(work: Path) -> None:
    _write_rivet_yaml(
        work,
        """source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
""",
    )


# ── CDC fixtures ───────────────────────────────────────────────────────────────
def fixture_cdc(work: Path) -> None:
    """Pin a checkpoint at the CURRENT binlog position, then make a small,
    deterministic backlog after it.

    The order matters: the checkpoint is the anchor, the three UPDATEs are the
    entire set of changes past it, so the bounded drain (the default) captures
    exactly three rows and exits inside the recording window. Without the
    anchor, rivet would tail from "now" and the GIF would record nothing.
    """
    # The compose `mysql` service creates `rivet` with ALL PRIVILEGES on the
    # `rivet` schema and nothing global, so both `SHOW MASTER STATUS` and the
    # binlog dump `rivet cdc` opens fail with `ERROR 1227` (SUPER / REPLICATION
    # CLIENT). Grant it here, as root and idempotently — otherwise this scenario
    # is only renderable on a machine where someone granted it by hand.
    # REPLICATION CLIENT reads the position, REPLICATION SLAVE opens the stream.
    mysql(root=True, sql="""
        GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'rivet'@'%';
        FLUSH PRIVILEGES;
    """).check("grant the binlog privileges to the `rivet` MySQL user")

    # Read the position AS THE TAPE'S USER, so a grant that did not take effect
    # surfaces here rather than as ERROR 1227 halfway through the recording.
    status = mysql("-N", "-B", sql="SHOW MASTER STATUS")
    status.check("SHOW MASTER STATUS (cdc fixture)")
    fields = status.stdout.splitlines()[0].split("\t") if status.stdout.strip() else []
    if len(fields) < 2 or not fields[1].strip().isdigit():
        # bash piped this through awk, which happily wrote an EMPTY cdc.ckpt when
        # the position was unreadable — rivet then re-anchored to "now" and the
        # recording showed a drain of zero changes.
        raise Fail(
            "could not read a binlog position from `SHOW MASTER STATUS` "
            f"(got {status.stdout.strip()!r})",
            hint="the compose MySQL needs log_bin=ON with binlog_format=ROW",
        )
    shell.atomic_write(
        work / "cdc.ckpt",
        '{"file":"%s","pos":%s}' % (fields[0].strip(), fields[1].strip()),
    )

    mysql(sql="""
        UPDATE content_items SET view_count = view_count + 1 WHERE id = 1;
        UPDATE content_items SET status = 'published'        WHERE id = 2;
        UPDATE content_items SET view_count = view_count + 1 WHERE id = 3;
    """).check("seed the CDC backlog (3 UPDATEs)")


def fixture_error_cdc_access(work: Path) -> None:
    # A SELECT-only user with no REPLICATION grant, so the tape can show the
    # actionable permission error. Root does the user management — rivet cannot.
    mysql(root=True, sql="""
        CREATE USER IF NOT EXISTS 'rivet_noperm'@'%' IDENTIFIED BY 'x';
        GRANT SELECT ON rivet.* TO 'rivet_noperm'@'%';
        FLUSH PRIVILEGES;
    """).check("create the rivet_noperm MySQL user")


def fixture_error_cdc_access_teardown(work: Path) -> None:
    dropped = mysql(root=True, sql="DROP USER IF EXISTS 'rivet_noperm'@'%';")
    if not dropped.ok:
        warn("error-cdc-access teardown: DROP USER returned non-zero (tolerated)")


def fixture_cdc_parallel(work: Path) -> None:
    # A full snapshot and a CDC stream of the SAME table in one run. The
    # snapshot is a LIMIT query so it finishes inside the window whatever the
    # table's real size; the CDC export reuses the anchored checkpoint, and
    # `until_current: true` (still a valid CONFIG key, unlike the removed CLI
    # flag) bounds it at the open-time log position.
    fixture_cdc(work)
    shell.atomic_write(
        work / "parallel.yaml",
        """source:
  type: mysql
  url: "mysql://rivet:rivet@127.0.0.1:3306/rivet"
exports:
  - name: content_items_snapshot
    query: "SELECT id, title, status, view_count FROM content_items LIMIT 50000"
    mode: full
    format: parquet
    destination: {type: local, path: ./output}
  - name: content_items_cdc
    table: content_items
    mode: cdc
    format: parquet
    cdc: {checkpoint: ./cdc.ckpt, until_current: true}
    destination: {type: local, path: ./output}
""",
    )


# ── scenario registry ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Scenario:
    name: str
    setup: Callable[[Path], None]
    teardown: Callable[[Path], None]
    opt_in: bool = False
    note: str = ""


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("basic", fixture_basic, _noop),
    Scenario("plan-apply", fixture_basic, _noop),
    Scenario("reconcile-repair", fixture_chunked, fixture_chunked_teardown),
    Scenario("init-scaffold", _noop, _noop),
    Scenario("check-verdict", fixture_check, _noop),
    Scenario("inspect", fixture_inspect, fixture_chunked_teardown),
    Scenario("chunked-progress", fixture_chunked_progress, fixture_chunked_teardown),
    Scenario("parallel-cards", fixture_parallel_cards, fixture_chunked_teardown),
    Scenario("incremental-cursor", fixture_incremental, fixture_chunked_teardown),
    Scenario("coalesce-cursor", fixture_coalesce, fixture_chunked_teardown),
    Scenario("discover-artifact", fixture_discover, _noop),
    Scenario("plan-campaign", fixture_campaign, fixture_chunked_teardown,
             note="seeds ~35M rows in rivet_gif.*"),
    Scenario("error-missing-table", fixture_error_missing_table, _noop),
    Scenario("error-config-typo", fixture_error_config_typo, _noop),
    Scenario("error-connection", fixture_error_connection, _noop),
    Scenario("cdc", fixture_cdc, _noop),
    Scenario("error-cdc-access", fixture_error_cdc_access, fixture_error_cdc_access_teardown),
    Scenario("cdc-parallel", fixture_cdc_parallel, _noop),
    # Opt-in: extra infrastructure the default run must not require.
    Scenario("pool-detect", fixture_pool_detect, _noop, opt_in=True,
             note="needs `docker compose --profile pool up -d pgbouncer proxysql`"),
    Scenario("doctor-gcs", fixture_gcs, fixture_gcs_teardown, opt_in=True,
             note="needs `gcloud auth application-default login` + a writable bucket"),
)

BY_NAME = {s.name: s for s in SCENARIOS}
DEFAULT_ORDER = tuple(s.name for s in SCENARIOS if not s.opt_in)


def resolve(names: Sequence[str] | None) -> list[Scenario]:
    """Names → scenarios, in the order given. `None` is the default set: every
    scenario except the two that need infrastructure beyond the repo stack."""
    wanted = list(names) if names else list(DEFAULT_ORDER)
    picked: list[Scenario] = []
    for name in wanted:
        scenario = BY_NAME.get(name)
        if scenario is None:
            raise Fail(
                f"unknown scenario: {name}",
                hint="known: " + ", ".join(sorted(BY_NAME)),
            )
        picked.append(scenario)
    return picked


# ── the render loop ────────────────────────────────────────────────────────────
def tape_env() -> dict[str, str]:
    """What the tapes' `Hide` preludes read: the release binaries' directory
    goes on PATH, and `$PSQL_BIN` / `$GCLOUD_BIN` let a tape script the source DB
    or the bucket without assuming a Homebrew layout."""
    return {
        "RIVET_BIN_DIR": str(BIN_DIR),
        "DATABASE_URL": DATABASE_URL,
        "PSQL_BIN": psql_bin(),
        "GCLOUD_BIN": gcloud_bin(),
    }


def render_one(scenario: Scenario, *, keep: bool = False) -> None:
    name = scenario.name
    work = Path(f"/tmp/rivet-gif-{name}")
    tape = HERE / f"{name}.tape"
    if not tape.is_file():
        raise Fail(f"no tape for scenario `{name}` at {tape}")

    # Audited first: a stale tape must not cost a 35M-row seed to discover.
    audit_tape(tape)

    shell.rm_rf(work)
    work.mkdir(parents=True, exist_ok=True)

    failure: BaseException | None = None
    try:
        log(f"[{name}] setup", tag=TAG)
        scenario.setup(work)

        # The tape is copied into the workdir so its relative `Output` (and every
        # relative config path it types) resolves there.
        shutil.copyfile(tape, work / tape.name)

        log(f"[{name}] rendering with vhs ...", tag=TAG)
        shell.stream(["vhs", tape.name], cwd=work, env=tape_env(), timeout=None).check(
            f"[{name}] vhs"
        )

        gif = work / f"{name}.gif"
        if not gif.is_file():
            raise Fail(
                f"[{name}] vhs did not produce {gif.name}", hint=f"inspect {work}"
            )
        destination = HERE / gif.name
        shutil.move(str(gif), destination)
        log(f"[{name}] -> {destination}", tag=TAG)
    except BaseException as exc:  # noqa: BLE001 — re-raised below, after teardown
        failure = exc

    # Teardown is unconditional: bash's `set -e` skipped it on any failure, so a
    # broken tape leaked its fixtures (a schema, a MySQL user) into the next
    # scenario's run. Setup lives inside the same guard, so a half-finished seed
    # is cleaned up too.
    log(f"[{name}] teardown", tag=TAG)
    try:
        scenario.teardown(work)
    except Fail as exc:
        if failure is None:
            failure = exc
        else:
            bad(f"[{name}] teardown also failed: {exc.message}")

    if failure is not None:
        raise failure
    if keep:
        log(f"[{name}] keeping {work}", tag=TAG)
    else:
        shell.rm_rf(work)


def render(names: list[str] | None = None, *, keep: bool = False) -> int:
    """Render `names` (default: every non-opt-in scenario, in registry order).

    Resolution happens before the prerequisite check so a typo'd scenario name
    fails immediately instead of after a release build.
    """
    scenarios = resolve(names)
    ensure_prereqs()
    for scenario in scenarios:
        render_one(scenario, keep=keep)
    log("done", tag=TAG)
    ok(f"rendered {len(scenarios)} scenario(s) into {HERE}")
    return 0


# ── CLI ────────────────────────────────────────────────────────────────────────
def _print_list() -> None:
    for scenario in SCENARIOS:
        marks = []
        if scenario.opt_in:
            marks.append("opt-in")
        if scenario.note:
            marks.append(scenario.note)
        suffix = f"  # {'; '.join(marks)}" if marks else ""
        print(f"{scenario.name}{suffix}")


def main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="render_gifs",
        description="Regenerate rivet's instructional GIFs with VHS.",
    )
    parser.add_argument(
        "scenarios", nargs="*",
        help="scenario names to render (default: every non-opt-in scenario)",
    )
    parser.add_argument(
        "--keep", action="store_true",
        help="keep each /tmp/rivet-gif-<name> workdir after a successful render",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_only",
        help="list the scenarios and exit",
    )
    args = parser.parse_args(argv)

    if args.list_only:
        _print_list()
        return 0
    return render(args.scenarios or None, keep=args.keep)


if __name__ == "__main__":
    shell.main(lambda: main_cli())
