"""Source-parity sweep (BATCH) + the stale-fixture pre-run sweep.

Two ports that share a name and nothing else:

* `dev/sweep/source_parity_sweep.sh` → `source_parity()`
  The BATCH half of rivet's strongest oracle. Per engine it seeds a hostile type
  fixture (1000 rows: low-cardinality, null-heavy, `DECIMAL(38,10)` differing in
  the 10th decimal, unicode/emoji, uuid, json, date), exports it with rivet, and
  compares a per-column profile computed from the SOURCE (direct DB query)
  against the DESTINATION parquet read by DuckDB. It does NOT trust rivet's own
  counters, which is the point: row loss, null injection, distinct collapse and
  decimal truncation all survive a self-oracle. Exit 1 on any mismatch.

  The CDC half is `dev/pytools/cdc_soak.py source-parity`, and this module
  deliberately IMPORTS its comparison core (`Tally`, `norm`, `chk`, `compare`,
  `ddv`, `dc_exec`) rather than re-deriving it — the two sweeps drifted apart in
  bash precisely because each carried its own copy of `norm`/`chk`.

* `dev/sweep-test-cruft.sh` → `test_cruft()`
  Drops fixture tables left behind by INTERRUPTED live runs. Live tests name
  fixtures `<prefix>_<pid>_<counter>` (`tests/common::unique_name`) and drop them
  from an RAII guard that does NOT fire when the test PROCESS is killed (nextest
  slow-timeout, SIGKILL, Ctrl-C — routine for the slow cloud suites). The
  `_<digits>_<digits>` suffix is what makes this safe: the persistent fixtures
  (users, orders, content_items, rivet_type_matrix, … from init.sql / seed.rs)
  never carry it. Best-effort per engine; a service that is down is skipped.

Usage:

    python3 dev/pytools/sweep.py source-parity [rivet-binary]
    python3 dev/pytools/sweep.py test-cruft

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH (each one a bug it shipped):

1. **No `trap`, so every early exit leaked.** Same gap as the CDC sibling: a
   Ctrl-C mid-sweep, or the `exit 2` preconditions, left `sweep_src` behind on
   whichever engines had been seeded plus the `mktemp -d` output tree (three
   engines × 1000 rows of parquet). Each engine's body is now `try/finally`, and
   the tempdir is removed in a `finally` around all three. (The batch sweep has
   no replication slot or capture instance to leak — that half of the CDC
   sibling's leak is CDC-only.)
2. **An empty source read no longer compares equal to an empty destination.**
   `MY()`/`MS()` sent the client's stderr to `/dev/null` (bug class 8), so a
   down engine produced `src=""`; the export then wrote no parquet, so `ddv`
   returned `""` too — and `chk` printed **`ok`** for a comparison of nothing
   against nothing. CONFIRMED by running the bash against this stack with the
   batch mssql container down: it printed **13 `ok` cells for an engine that was
   not running**, then exited 1 on the single "rivet FAILED" tally — i.e. it
   reported `SILENT-CORRUPTION DETECTED` (which
   `tests/live/live_source_parity_sweep.rs` reads as a real regression) for a
   service that was merely absent. Now: a seed that cannot run marks that engine
   NOT AVAILABLE, prints no cells for it, and — unless a genuine mismatch was
   found elsewhere, which outranks it — ends as `sweep INCOMPLETE` with **exit
   2**, the code that wrapper already documents as "a service down" and turns
   into a SKIP. A destination with no `.parquet` is likewise a named failure
   rather than an agreement.
3. `test_cruft`'s PostgreSQL arm computed `n` in a `DO` block and `RAISE
   NOTICE`d it into `>/dev/null 2>&1`, then printed a fixed "postgres: swept" —
   the count it went to the trouble of keeping was discarded. The NOTICE is now
   read back and reported (`postgres: swept (N stale fixtures dropped)`); the
   `quote_ident` server-side quoting of the original is kept.
4. `test_cruft`'s MySQL arm was `docker exec … -e "SELECT 'DROP …'" | docker exec
   -i … mysql`, i.e. bug class 5: the verdict came from the LAST stage, so a
   failed *generator* still printed "mysql: swept". The names are now fetched,
   quoted and dropped in two separate checked steps, and a failure is reported
   loudly instead of claimed as a sweep.

KNOWN-WEAK ORACLE, kept as-is for fidelity: `norm()` (imported from `cdc_soak`)
strips trailing zeros — `sed 's/0*$//'` — so the two sides can agree across
engines that render decimals differently. It also means `1000` normalises to `1`,
so the comparison is blind to some magnitude differences in the LAST field of a
value. Reproduced bug-for-bug rather than tightened, because tightening it would
change which cells fail.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Sequence

if __package__:
    from . import cdc_soak, cdc_stand, shell
else:  # executed as a plain script: `python3 dev/pytools/sweep.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cdc_soak  # type: ignore[no-redef]
    import cdc_stand  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT

USAGE = (
    "usage: dev/pytools/sweep.py source-parity [rivet-binary]\n"
    "       dev/pytools/sweep.py test-cruft"
)

# The comparison core is the CDC sibling's, on purpose — one `norm`, one `chk`,
# one column list for both sweeps.
Tally = cdc_soak.Tally
compare = cdc_soak.compare
norm = cdc_soak.norm
ddv = cdc_soak.ddv
SWEEP_ROWS = cdc_soak.SWEEP_ROWS

# ── batch stack (the OTHER ports than the cdc profile: 5432 / 3306 / 1433) ─────
PG_URL = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
MY_URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
MS_URL = f"sqlserver://sa:{cdc_stand.SA_PASSWORD}@127.0.0.1:1433/rivet"


def export_parquet(t: Tally, rivet: Path, engine: str, url: str, query: str, out: Path) -> None:
    """Write `<out>.yaml` and run one `mode: full` export into `<out>`.

    `atomic_write` rather than `printf > "$4.yaml"`: the redirect truncates the
    config before the generator writes it, so an interrupted write left a
    half-config that rivet then reported as a parse error.
    """
    cfg = Path(f"{out}.yaml")
    shell.atomic_write(
        cfg,
        f'source: {{ type: {engine}, url: "{url}" }}\n'
        "exports:\n"
        "  - name: s\n"
        f'    query: "{query}"\n'
        "    mode: full\n"
        "    format: parquet\n"
        f'    destination: {{ type: local, path: "{out}" }}\n',
    )
    if not shell.run([str(rivet), "run", "--config", str(cfg)], cwd=ROOT, timeout=None).ok:
        print("  rivet FAILED")
        t.failed += 1


def _unavailable(engine: str, proc: shell.Proc, hint: str) -> str:
    """A down engine is an ENVIRONMENT gap (exit 2), not a corruption finding.

    The bash reported it as corruption: with the client's stderr in `/dev/null`
    the source profile came back empty, the export failed ("rivet FAILED"), and
    the run ended on `SILENT-CORRUPTION DETECTED` + exit 1 — which
    `tests/live/live_source_parity_sweep.rs` reads as a real regression. Exit 2 is
    the code that wrapper turns into a SKIP, and it is what "a service down"
    already meant in its own comment.
    """
    detail = (proc.stderr or proc.stdout).strip().splitlines()
    reason = detail[-1] if detail else f"exit {proc.returncode}"
    # stdout, because the Rust wrapper echoes stdout when it decides to skip.
    print(f"  {engine} NOT AVAILABLE: {reason}")
    shell.skip(f"{engine}: not available — {hint}")
    return f"{engine} not available ({reason})"


def _destination_readable(t: Tally, out: Path) -> bool:
    """Is there anything to compare against?

    Without this, a run that wrote NO parts compares an empty source profile
    against an empty destination profile and `chk` reports `ok` for all thirteen
    cells — the vacuous pass this sweep exists to prevent.
    """
    if sorted(out.glob("*.parquet")):
        return True
    shell.bad(f"{out.name}: no parquet parts written — nothing to compare against")
    t.failed += 1
    return False


# ── Postgres ───────────────────────────────────────────────────────────────────
PG_SEED = (
    "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
    "k INT, maybe_null INT, amount DECIMAL(38,10), label TEXT, uid UUID, ts TIMESTAMP, "
    "payload JSONB, d DATE);\n"
    "INSERT INTO sweep_src SELECT g, g%50, CASE WHEN g%3=0 THEN NULL ELSE g END, "
    "(g::numeric*0.0000000001), CASE WHEN g%7=0 THEN NULL ELSE 'rôw_😀_'||(g%100) END, "
    "gen_random_uuid(), timestamp '2020-01-01'+(g||' minutes')::interval, "
    f"json_build_object('k',g%50,'v',g)::jsonb, date '2020-01-01'+g FROM generate_series(1,{SWEEP_ROWS}) g;"
)


def _sweep_postgres(t: Tally, rivet: Path, out: Path) -> str | None:
    """Returns None when the engine ran, or a reason string when it was down."""

    def pg(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec("postgres", *cdc_stand.psql_argv(q), timeout=timeout)

    print("== BATCH postgres ==")
    # Checked where the bash had `>/dev/null`: a seed that cannot run is the
    # difference between "this engine is corrupting values" and "this engine is
    # not up", and the bash reported the second as the first.
    seeded = pg(PG_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("postgres", seeded, "docker compose up -d postgres")
    try:
        export_parquet(t, rivet, "postgres", PG_URL, "SELECT * FROM sweep_src", out / "pg")
        if _destination_readable(t, out / "pg"):
            compare(
                t,
                lambda c: pg(
                    f"SELECT count(*)||'/'||count({c})||'/'||count(distinct {c}) FROM sweep_src"
                ).stdout.strip(),
                lambda c: pg(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
                f"read_parquet('{out / 'pg'}/*.parquet')",
            )
    finally:
        pg("DROP TABLE sweep_src;")
    return None


# ── MySQL ──────────────────────────────────────────────────────────────────────
MY_SEED = (
    "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
    "k INT, maybe_null INT, amount DECIMAL(38,10), label VARCHAR(60), uid CHAR(36), "
    "ts DATETIME, payload JSON, d DATE);\n"
    "SET SESSION cte_max_recursion_depth=4000;\n"
    "INSERT INTO sweep_src WITH RECURSIVE seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq "
    f"WHERE g<{SWEEP_ROWS}) SELECT g,g%50,IF(g%3=0,NULL,g),CAST(g AS DECIMAL(38,10))"
    "*0.0000000001,IF(g%7=0,NULL,CONCAT('rôw_😀_',g%100)),UUID(),"
    "TIMESTAMP('2020-01-01')+INTERVAL g MINUTE,JSON_OBJECT('k',g%50,'v',g),"
    "DATE('2020-01-01')+INTERVAL g DAY FROM seq;"
)


def _sweep_mysql(t: Tally, rivet: Path, out: Path) -> str | None:
    def my(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec("mysql", *cdc_stand.mysql_argv(q), timeout=timeout)

    print("== BATCH mysql ==")
    seeded = my(MY_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("mysql", seeded, "docker compose up -d mysql")
    try:
        export_parquet(t, rivet, "mysql", MY_URL, "SELECT * FROM sweep_src", out / "my")
        if _destination_readable(t, out / "my"):
            compare(
                t,
                lambda c: my(
                    f"SELECT CONCAT(count(*),'/',count({c}),'/',count(distinct {c})) FROM sweep_src"
                ).stdout.strip(),
                lambda c: my(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
                f"read_parquet('{out / 'my'}/*.parquet')",
            )
    finally:
        my("DROP TABLE sweep_src;")
    return None


# ── SQL Server ─────────────────────────────────────────────────────────────────
MS_SEED = (
    "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src; "
    "CREATE TABLE dbo.sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, "
    "amount DECIMAL(38,10), label NVARCHAR(60), uid UNIQUEIDENTIFIER, ts DATETIME2, "
    "payload NVARCHAR(200), d DATE);\n"
    ";WITH seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE "
    f"g<{SWEEP_ROWS}) INSERT INTO dbo.sweep_src SELECT g,g%50,"
    "CASE WHEN g%3=0 THEN NULL ELSE g END,CAST(g*0.0000000001 AS DECIMAL(38,10)),"
    "CASE WHEN g%7=0 THEN NULL ELSE N'rôw_'+CAST(g%100 AS NVARCHAR(10)) END,NEWID(),"
    "DATEADD(MINUTE,g,CAST('2020-01-01' AS DATETIME2)),"
    "N'{\"k\":'+CAST(g%50 AS NVARCHAR(10))+N',\"v\":'+CAST(g AS NVARCHAR(10))+N'}',"
    "DATEADD(DAY,g,CAST('2020-01-01' AS DATE)) FROM seq OPTION (MAXRECURSION 0);"
)


def _sweep_mssql(t: Tally, rivet: Path, out: Path) -> str | None:
    def ms(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec(
            "mssql", *cdc_stand.sqlcmd_argv(q, wide=True, nocount=True), timeout=timeout
        )

    print("== BATCH mssql ==")
    seeded = ms(MS_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("mssql", seeded, "docker compose up -d mssql")
    try:
        export_parquet(t, rivet, "mssql", MS_URL, "SELECT * FROM dbo.sweep_src", out / "ms")
        if _destination_readable(t, out / "ms"):
            compare(
                t,
                lambda c: ms(
                    f"SELECT CAST(count(*) AS VARCHAR)+'/'+CAST(count({c}) AS VARCHAR)+'/'"
                    f"+CAST(count(distinct {c}) AS VARCHAR) FROM dbo.sweep_src"
                ).stdout.strip(),
                lambda c: ms(
                    f"SELECT CAST(COALESCE(SUM({c}),0) AS VARCHAR(64)) FROM dbo.sweep_src"
                ).stdout.strip(),
                f"read_parquet('{out / 'ms'}/*.parquet')",
            )
    finally:
        ms("DROP TABLE dbo.sweep_src;")
    return None


def source_parity(binary: str | Path | None = None) -> int:
    """The BATCH value-parity sweep across PostgreSQL, MySQL and SQL Server.

    All three engines always run, as in the CDC sibling: a sweep that quietly
    drops an engine is worse than one that fails, because the missing engine's
    decode path is exactly where a silent-corruption bug survives.

    Exit codes are the ones `tests/live/live_source_parity_sweep.rs` reads: 0
    clean, 1 corruption, **2 environment/setup missing** (which that wrapper
    turns into a SKIP rather than a failure).
    """
    spelled = str(binary or os.environ.get("RIVET") or "target/debug/rivet")
    rivet = Path(spelled)
    if not rivet.is_absolute():
        rivet = ROOT / rivet
    if not (rivet.is_file() and os.access(rivet, os.X_OK)):
        raise shell.Fail(
            f"rivet not at {spelled} (cargo build --bin rivet, or set RIVET=)", code=2
        )
    if not shell.have("duckdb"):
        raise shell.Fail("duckdb CLI not on PATH", code=2)

    t = Tally()
    out = Path(tempfile.mkdtemp(prefix="rivet-parity-batch-"))
    print("###### rivet source-parity sweep (batch) ######")
    missing: list[str] = []
    try:
        for engine_sweep in (_sweep_postgres, _sweep_mysql, _sweep_mssql):
            reason = engine_sweep(t, rivet, out)
            if reason:
                missing.append(reason)
    finally:
        # The bash's `rm -rf "$OUT"` ran only on the happy path.
        shutil.rmtree(out, ignore_errors=True)

    print("###############################################")
    print(f"BATCH: {t.match}/{t.total} independent checks matched the source ({t.failed} failed)")
    # Corruption outranks an incomplete sweep: a mismatch that DID happen is a
    # finding regardless of which other engine was down.
    if t.failed:
        raise shell.Fail("SILENT-CORRUPTION DETECTED")
    if missing:
        raise shell.Fail("sweep INCOMPLETE: " + "; ".join(missing), code=2)
    return 0


# ══ dev/sweep-test-cruft.sh ════════════════════════════════════════════════════
# unique_name suffix: `_<pid>_<counter>` at end of name. Both engines use the
# same pattern; it is what keeps this sweep off the persistent fixtures.
FIXTURE_PAT = "_[0-9]+_[0-9]+$"

PG_CRUFT_CONTAINER = "rivet-postgres-1"
MY_CRUFT_CONTAINER = "rivet-mysql-1"

# Kept verbatim from the bash — the DROP is built server-side with
# `quote_ident`, which is the safe way to name a table that came out of the
# catalogue. Only the NOTICE's fate changes: it is read instead of discarded.
PG_CRUFT_SQL = f"""DO $$
DECLARE r RECORD; n int := 0;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables
           WHERE schemaname='public' AND tablename ~ '{FIXTURE_PAT}' LOOP
    EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    n := n + 1;
  END LOOP;
  RAISE NOTICE 'postgres: dropped % stale fixtures', n;
END $$;
"""

_NOTICE_N = re.compile(r"dropped\s+([0-9]+)\s+stale fixtures")


def _container_up(name: str) -> bool:
    """`docker exec <c> true 2>/dev/null` — the bash's liveness gate."""
    return shell.docker_exec(name, "true", timeout=30).ok


def _sweep_pg_cruft() -> None:
    p = shell.docker_exec(
        PG_CRUFT_CONTAINER,
        "psql", "-U", "rivet", "-d", "rivet", "-q", "-v", "ON_ERROR_STOP=0",
        stdin=PG_CRUFT_SQL,
    )
    m = _NOTICE_N.search(p.out)
    if not p.ok:
        tail = (p.stderr or p.stdout).strip().splitlines()
        shell.warn(f"  postgres: sweep FAILED: {tail[-1] if tail else p.returncode}")
        return
    n = m.group(1) if m else "?"
    print(f"  postgres: swept ({n} stale fixtures dropped)")


def _my_quote(name: str) -> str:
    """MySQL identifier quoting: backticks, internal backticks doubled.

    The bash built the DROP statements with `CONCAT` inside MySQL, which is
    equally safe; doing it here is what lets the generator's exit status be
    checked instead of being swallowed by the pipe's last stage.
    """
    return "`" + name.replace("`", "``") + "`"


def _sweep_my_cruft() -> None:
    listing = shell.docker_exec(
        MY_CRUFT_CONTAINER,
        *cdc_stand.mysql_argv(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='rivet' AND table_name REGEXP '{FIXTURE_PAT}'"
        ),
    )
    if not listing.ok:
        tail = (listing.stderr or listing.stdout).strip().splitlines()
        shell.warn(f"  mysql: sweep FAILED (listing): {tail[-1] if tail else listing.returncode}")
        return
    names = [line.strip() for line in listing.stdout.splitlines() if line.strip()]
    if names:
        script = "".join(f"DROP TABLE IF EXISTS {_my_quote(n)};\n" for n in names)
        drop = shell.docker_exec(
            MY_CRUFT_CONTAINER, "mysql", "-urivet", "-privet", "rivet", stdin=script
        )
        if not drop.ok:
            tail = (drop.stderr or drop.stdout).strip().splitlines()
            shell.warn(f"  mysql: sweep FAILED (drop): {tail[-1] if tail else drop.returncode}")
            return
    print(f"  mysql: swept ({len(names)} stale fixtures dropped)")


def test_cruft() -> int:
    """Drop stale `_<pid>_<counter>` fixtures on the batch engines.

    Best-effort by design (the nextest setup script in `.config/nextest.toml`
    runs it before every live run, and `make sweep-test-db` runs it by hand): a
    down engine is skipped, a failed drop is reported loudly, and the exit code
    stays 0 either way so a housekeeping hiccup never aborts the live suite.

    SQL Server is still a follow-up, exactly as the bash left it: T-SQL has no
    regex, so a precise `_<pid>_<counter>$` match needs PATINDEX gymnastics or a
    CLR function, and a loose `LIKE` risks dropping a real fixture. The mssql
    live suites leak far less (fewer, slower), so this is deferred rather than
    done loosely.
    """
    print("sweep-test-cruft: dropping stale unique_name fixtures (suffix _<pid>_<counter>)")

    if _container_up(PG_CRUFT_CONTAINER):
        _sweep_pg_cruft()
    else:
        print("  postgres: not up — skipped")

    if _container_up(MY_CRUFT_CONTAINER):
        _sweep_my_cruft()
    else:
        print("  mysql: not up — skipped")

    print("sweep-test-cruft: done")
    return 0


# ══ CLI ════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else ""
    rest = args[1:]

    if cmd in ("source-parity", "source_parity", "parity"):
        return source_parity(rest[0] if rest else None)
    if cmd in ("test-cruft", "test_cruft", "cruft"):
        return test_cruft()

    print(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
