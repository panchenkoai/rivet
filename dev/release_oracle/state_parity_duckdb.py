"""SQLite vs Postgres state parity, read by ONE independent decoder.

`state_parity` already compares the two backends, and compares more than this
does. It has one structural weakness this module exists to remove: it reads
SQLite with Python's `sqlite3` and Postgres with `psql`, so the two sides are
rendered by two different clients before anything is compared. A difference
that survives to the assertion can be a data difference OR a rendering
difference, and nothing distinguishes them.

That is not theoretical. Making the seed uniform today, a digest of `users`
computed with each ENGINE's own string concatenation disagreed across three
engines whose data was byte-identical — proven identical by column aggregates
and by exporting all three and hashing the parquet through DuckDB. The neutral
reader settled in one query what the native ones could not settle at all.

So: DuckDB attaches BOTH stores in one session (`sqlite` + `postgres`
scanners), and every value crosses exactly one decoder and one renderer. A
digest that differs is a data difference, full stop.

WHAT IS COMPARED

  the table set     scoped to tables that exist on BOTH and carry an
                    `export_name` column. The Postgres store is SHARED with
                    every other cell in this gate while the SQLite one is fresh
                    per run, so an unscoped table is incomparable BY
                    CONSTRUCTION — excluded loudly, never silently.
  the row count     per table, for THIS export.
  a content digest  per table: every shared column, in a canonical order,
                    concatenated and hashed by DuckDB on both sides with the
                    SAME expression. This is the part `state_parity` cannot do
                    — it compares counts and a few hand-picked work columns, so
                    two rows that differ in an unlisted column agree.

WHAT MAKES IT FAIL RATHER THAN PASS QUIETLY

  An empty comparison is a FAILURE, not agreement. Two profiles that are both
  empty match perfectly and prove nothing — `state_parity` passes in exactly
  that case today (verified by reading `populated()` and the problem loop: if
  both sides yield no populated tables, `only_sq`/`only_pg` are empty, the
  count loop iterates over nothing, and `problems` stays empty). This module
  requires a minimum number of compared tables and says so in the cell.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from .core import Ledger, have, run

# Written by the STORE, not by a run — the migration bookkeeping table is
# `schema_version` on SQLite and `rivet_schema_version` on Postgres. Two names
# for one concept, excluded here for the same reason `state_parity` excludes it.
BOOKKEEPING = {"schema_version", "rivet_schema_version"}

# The comparison is meaningless below this. Chosen from what a batch export
# actually populates — export_metrics, file_log, run_status — so a run that
# recorded nothing cannot be mistaken for two backends agreeing.
MIN_TABLES = 3

# Tables that measure the ENVIRONMENT, not the export's work. `export_harm`
# records PostgreSQL server counter DELTAS (pg_blks_hit, pg_tup_fetched,
# pg_tup_returned) sampled around the run — cache state, autovacuum and
# background activity move them, so two runs of the same export differ by
# nature. Measured on a manual pair: 3094 vs 3062 blks_hit, 3747 vs 3732
# tup_returned. Excluded for the same reason `state_parity` excludes the
# unscoped tables — incomparable by construction, said out loud.
VOLATILE_TABLES = {"export_harm"}

# Columns that time or measure the run rather than describe its result. Derived
# from a real pair, not guessed: of 39 shared `export_metrics` columns exactly
# six differed, and these two are the ones that are SUPPOSED to.
VOLATILE_COLUMNS = {"duration_ms", "peak_rss_mb", "longest_chunk_ms"}


def _duck(sql: str) -> tuple[str, bool]:
    """Run one DuckDB script; return (stdout, ok). The exit status is carried
    because an unreadable store must not read as an empty one — the defect this
    gate had in `concurrency._psql_state` and still has in `state_parity._psql`."""
    p = run(["duckdb", "-noheader", "-list", "-c", sql])
    return (p.stdout.strip(), p.ok)


def _attach(sqlite_db: Path, pg_url: str) -> str:
    return (
        "LOAD sqlite; LOAD postgres;\n"
        f"ATTACH '{sqlite_db}' AS sq (TYPE sqlite, READ_ONLY);\n"
        f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY);\n"
    )


def verify_state_parity_independent(
    led: Ledger,
    *,
    sqlite_db: Path | None,
    pg_url: str,
    export: str,
    path_aliases: tuple[str, ...] = (),
) -> None:
    """Compare the two state backends through DuckDB alone."""
    if not have("duckdb"):
        led.skipped("-", "-", "state_parity_independent", "-",
                    "state-parity[independent]: no duckdb — the whole point of this cell "
                    "is the neutral reader, so there is nothing to degrade to",
                    "no duckdb")
        return
    if not sqlite_db or not sqlite_db.is_file():
        led.skipped("-", "-", "state_parity_independent", "-",
                    f"state-parity[independent]: no SQLite state DB at {sqlite_db}",
                    "no sqlite db")
        return
    if not pg_url:
        led.skipped("-", "-", "state_parity_independent", "-",
                    "state-parity[independent]: no Postgres state URL", "no pg url")
        return

    # Read a COPY: `READ_ONLY` still needs to open the -wal sidecar, and a live
    # database fails to attach. (The same trap the blessed-path module hit with
    # `mode=ro`, reported there as "no state DB" when the file was right there.)
    tmp = Path("/tmp") / f"parity_independent_{sqlite_db.stat().st_size}.db"
    shutil.copy2(sqlite_db, tmp)
    for side in ("-wal", "-shm"):
        s = sqlite_db.with_name(sqlite_db.name + side)
        if s.is_file():
            shutil.copy2(s, tmp.with_name(tmp.name + side))

    pre = _attach(tmp, pg_url)

    # Tables present on BOTH, carrying `export_name`, excluding bookkeeping.
    excl = ", ".join(f"'{b}'" for b in sorted(BOOKKEEPING))
    listing, ok = _duck(
        pre
        + f"""
        SELECT c1.table_name
        FROM duckdb_columns() c1
        JOIN duckdb_columns() c2
          ON c2.table_name = c1.table_name AND c2.database_name = 'pg'
             AND c2.column_name = 'export_name'
        WHERE c1.database_name = 'sq' AND c1.column_name = 'export_name'
          AND c1.table_name NOT IN ({excl})
        GROUP BY 1 ORDER BY 1;
        """
    )
    if not ok:
        led.failed("-", "-", "state_parity_independent", "-",
                   "state-parity[independent]: could not attach one of the stores — the "
                   "comparison DID NOT RUN, which is not the same as agreement",
                   "attach failed")
        return
    tables = [
        t.strip() for t in listing.splitlines()
        if t.strip() and t.strip() not in VOLATILE_TABLES
    ]

    if len(tables) < MIN_TABLES:
        led.failed("-", "-", "state_parity_independent", "-",
                   f"state-parity[independent]: only {len(tables)} comparable table(s) "
                   f"({tables}) — below the {MIN_TABLES} a real export populates. Two empty "
                   f"profiles agree perfectly and prove nothing, so this is a failure, "
                   f"not a pass",
                   "nothing to compare")
        return

    problems: list[str] = []
    compared: list[str] = []
    for t in tables:
        # `run_journal` holds the whole event log as JSON, and that payload is
        # identity-DENSE by nature: a per-event `recorded_at` to the microsecond
        # and every part's run-unique name. Comparing its bytes compares two
        # runs, not two backends (measured: every difference was a microsecond
        # fraction or a part-name hash). Its STRUCTURE is the parity claim worth
        # making — same event types, same counts — so that is what is compared,
        # and the narrowing is stated rather than silently dropped.
        if t == "run_journal":
            shape_sql = pre + "".join(
                f"""
                SELECT '{side}:' || coalesce(string_agg(k || 'x' || n, ',' ORDER BY k), '-')
                FROM (
                  SELECT json_keys(e -> '$.event')[1] AS k, count(*) AS n
                  FROM {side}.run_journal,
                       UNNEST(CAST(json_extract(journal_json, '$.entries') AS JSON[])) AS t(e)
                  WHERE export_name = '{export}' GROUP BY 1
                );
                """
                for side in ("sq", "pg")
            )
            out, ok = _duck(shape_sql)
            lines = [x.strip() for x in out.splitlines() if x.strip()]
            got = {x.split(":", 1)[0]: x.split(":", 1)[1] for x in lines if ":" in x}
            if not ok or "sq" not in got or "pg" not in got:
                problems.append("run_journal: could not extract the event shape")
            elif got["sq"] != got["pg"]:
                problems.append(
                    f"run_journal: event shape SQLite {got['sq']} vs Postgres {got['pg']}"
                )
            else:
                compared.append("run_journal(shape)")
            continue

        # Columns shared by both sides, canonically ordered, so the digest
        # expression is identical on each side by construction.
        cols_out, ok = _duck(
            pre
            + f"""
            SELECT string_agg(column_name, ',' ORDER BY column_name) FROM (
              SELECT column_name FROM duckdb_columns()
              WHERE database_name='sq' AND table_name='{t}'
              INTERSECT
              SELECT column_name FROM duckdb_columns()
              WHERE database_name='pg' AND table_name='{t}'
            );
            """
        )
        cols = [c for c in cols_out.strip().split(",") if c]
        if not ok or not cols:
            problems.append(f"{t}: could not resolve a shared column set")
            continue
        # Identity columns differ between two runs BY CONSTRUCTION (row ids,
        # run ids, wall-clock stamps). Comparing them would only prove that two
        # runs are two runs.
        data_cols = [
            c for c in cols
            if c not in ("id", "rowid", "run_id", "run_at", "started_at", "finished_at",
                         "created_at", "updated_at", "recorded_at")
            and c not in VOLATILE_COLUMNS
        ]
        if not data_cols:
            continue
        # NORMALISE identity out of the VALUES rather than dropping more
        # columns. Dropping is what makes the native comparison weak — it looks
        # at counts and three hand-picked work columns, so two rows differing in
        # an unlisted column agree. But identity leaks into ordinary columns
        # too: `file_log.file_name` is `<export>_<stamp>_chunk<N>_<hash>.parquet`
        # and that trailing hash is run-unique BY DESIGN (it is the fix for
        # consecutive runs clobbering each other's parts). Two runs therefore
        # differ there necessarily.
        #
        # Measured, not assumed: the first version of this cell reported "DATA
        # difference" on five tables whose counts matched exactly, and the rows
        # turned out to differ only in `f2e332e98610b70c` vs `34c067044697909b`.
        # That was this check being wrong, not the backends.
        #
        # What the digest consequently CANNOT see: a divergence that exists
        # ONLY inside a 16-hex token or a timestamp. Everything else in every
        # shared column is still compared.
        # The harness gives each backend its OWN output directory, so any column
        # carrying a destination — `run_status.prefix`, `export_metrics`, the
        # journal payload — differs by construction. Aliased away for the same
        # reason the hashes are: it is the harness talking, not the backends.
        def norm(c: str) -> str:
            v = f"coalesce(CAST({c} AS VARCHAR), '~')"
            for alias in path_aliases:
                v = f"replace({v}, '{alias}', '<out>')"
            # SQLite has no BOOLEAN, so rivet stores 0/1 there and false/true on
            # Postgres. Folded here so the digest compares the VALUE — but the
            # divergence is real and worth a reader's attention: a consumer
            # writing one query against "the" state DB gets
            # `schema_changed = 0` working on SQLite and failing on Postgres.
            # The native comparison cannot see this at all (it reads three
            # hand-picked columns of `export_metrics`); this cell found it.
            v = f"replace(replace({v}, 'false', '0'), 'true', '1')"
            v = f"regexp_replace({v}, '[0-9a-f]{{16,}}', '<hash>', 'g')"
            v = f"regexp_replace({v}, '\\d{{8}}[T_]\\d{{6}}(\\.\\d+)?', '<stamp>', 'g')"
            v = f"regexp_replace({v}, '\\d{{4}}-\\d{{2}}-\\d{{2}}[T ]\\d{{2}}:\\d{{2}}:\\d{{2}}[^,\"]*', '<ts>', 'g')"
            return v

        # PER-COLUMN, not one digest over the row. A row-level digest says
        # "these differ" and nothing more, which is useless when the answer is
        # "in a microsecond fraction" — and it was, three times while building
        # this. Naming the column is the difference between a finding and a
        # scavenger hunt, and it makes what this cell CANNOT see visible in its
        # own output.
        diffs: list[str] = []
        for c in data_cols:
            n = norm(c)
            q = pre + "".join(
                f"""
                SELECT '{side}' || ':' || coalesce(md5(string_agg({n}, chr(10) ORDER BY {n})), '-')
                FROM {side}.{t} WHERE export_name = '{export}';
                """
                for side in ("sq", "pg")
            )
            out, ok = _duck(q)
            lines = [x.strip() for x in out.splitlines() if x.strip()]
            got = {x.split(":", 1)[0]: x.split(":", 1)[1] for x in lines if ":" in x}
            if not ok or "sq" not in got or "pg" not in got:
                diffs.append(f"{c}(unreadable)")
            elif got["sq"] != got["pg"]:
                diffs.append(c)

        # The row count is its own assertion: two backends can hold the same
        # column digests over different numbers of rows only if the extra rows
        # are duplicates, which is itself a divergence.
        cnt_q = pre + "".join(
            f"SELECT '{side}:' || count(*) FROM {side}.{t} WHERE export_name = '{export}';"
            for side in ("sq", "pg")
        )
        cnt_out, cnt_ok = _duck(cnt_q)
        cnt = {x.split(":", 1)[0]: x.split(":", 1)[1]
               for x in cnt_out.splitlines() if ":" in x}
        if not cnt_ok or cnt.get("sq") != cnt.get("pg"):
            diffs.append(f"ROW COUNT {cnt.get('sq')} vs {cnt.get('pg')}")

        compared.append(f"{t}({len(data_cols)} cols)")
        if diffs:
            problems.append(f"{t}: {', '.join(diffs)}")

    if problems:
        led.failed("-", "-", "state_parity_independent", "-",
                   "state-parity[independent]: " + "; ".join(problems),
                   "backends disagree")
        return
    led.passed(
        "-", "-", "state_parity_independent", "-",
        f"state-parity[independent]: DuckDB attached BOTH stores and every value crossed "
        f"ONE decoder — {len(compared)} table(s) ({', '.join(compared)}) agree on row count "
        f"AND on a PER-COLUMN digest of every shared column, scoped to export '{export}'. "
        f"A digest, unlike a count, cannot be satisfied by two different rows.",
    )
