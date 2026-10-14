"""The independent reader: one DuckDB session that can see every store rivet writes.

Why this is central rather than per-cell: an oracle only earns the name if it
shares NO code with the thing it grades, and the cheapest way to break that is to
let each cell open its own reader with its own extension list and its own idea of
how to reach BigQuery. Then "the oracle says 1000 rows" means something slightly
different per cell, and a cell that silently failed to attach reads as agreement.

So: one place that attaches, one place that names the env vars, and attach
failures are LOUD (`Oracle` raises) — never a silent fall back to "no rows".

The BigQuery half is the DuckDB community extension, which reads the warehouse
over the BigQuery Storage API as the operator's own ADC identity. That makes it a
second implementation from rivet's loader AND from `bq` — the CLI the golden
stage already shells out to (`dev/release_oracle/bigquery.py`). Two independent
readers disagreeing is the finding; one reader agreeing with itself is not.
"""

from __future__ import annotations

import os
import re
import time

import duckdb

#: The env vars the release gate already uses (dev/release-oracle/matrix.yaml).
BQ_PROJECT_ENV = "BQ_ORACLE_PROJECT"
BQ_DATASET_ENV = "BQ_ORACLE_DATASET"

#: A read may be tried this many times, and all tries and pauses together may take this long.
READ_TRIES, READ_BUDGET_S = 3, 180.0


#: libcurl codes a BigQuery read dies of without a verdict: couldn't connect (7), timeout (28),
#: SSL connect (35), send/recv failure (55/56), and an empty reply (52).
TRANSIENT_CURL = re.compile(r"CURL error \[(7|28|35|52|55|56)\]")


#: Where the DuckDB `snowflake` extension looks for its ADBC driver before its own extension directory.
SF_DRIVER_ENV = "SNOWFLAKE_ADBC_DRIVER_PATH"
#: The DuckDB secret every Snowflake read names.
SF_SECRET = "sf_login"


def snowflake_login(connection: str) -> dict:
    """The parameters of one `snow` connection, from the CLI's own listing (the operator's config, never a copy of it)."""
    import json
    import subprocess

    out = subprocess.run(["snow", "connection", "list", "--format", "json"], capture_output=True, text=True, check=True).stdout
    for c in json.loads(out[out.index("["):]):
        if c.get("connection_name") == connection:
            return c.get("parameters") or {}
    raise RuntimeError(f"Snowflake oracle: `snow` has no connection named `{connection}`")


def snowflake_secret(login: dict, database: str, warehouse: str) -> str:
    """The `CREATE SECRET` of a key-pair login; any other authenticator is refused by name."""
    key = login.get("private_key_file") or login.get("private_key_path")
    if not key or not login.get("account") or not login.get("user"):
        raise RuntimeError("Snowflake oracle needs a key-pair `snow` connection (account, user, private_key_file)")
    q = lambda v: "'" + str(v).replace("'", "''") + "'"  # noqa: E731
    return (f"CREATE OR REPLACE SECRET {SF_SECRET} (TYPE snowflake, ACCOUNT {q(login['account'])}, USER {q(login['user'])}, "
            f"AUTH_TYPE 'key_pair', PRIVATE_KEY {q(key)}, DATABASE {q(database)}, WAREHOUSE {q(warehouse)})")


class OracleUnavailable(Exception):
    """The oracle's own transport failed on every try: the cell graded nothing, which is not a product verdict."""


def transient(e: BaseException) -> bool:
    """Whether a DuckDB error is the reader's transport (curl, a dropped BigQuery Storage read session), not the data."""
    s = str(e)
    return bool(TRANSIENT_CURL.search(s)) or bool(
        "read session" in s and re.search(r"\b(CANCELLED|UNAVAILABLE|DEADLINE_EXCEEDED|ABORTED)\b", s))


def retry(read, *, tries: int = READ_TRIES, budget: float = READ_BUDGET_S, pause: float = 5.0,
          clock=time.monotonic, sleep=time.sleep):
    """`read()`, again while it fails in transport, within `tries` and `budget` seconds; then `OracleUnavailable`."""
    start = clock()
    for attempt in range(1, tries + 1):
        try:
            return read()
        except duckdb.Error as e:
            if not transient(e):
                raise
            spent = clock() - start
            if attempt == tries or spent + pause >= budget:
                raise OracleUnavailable(f"{attempt} of {tries} tries in {spent:.0f}s (budget {budget:.0f}s): "
                                        f"{str(e)[:300]}") from e
            sleep(pause)


def bq_target() -> tuple[str, str] | None:
    """`(project, dataset)` when both env vars are set, else `None` (the cell SKIPs)."""
    proj = os.environ.get(BQ_PROJECT_ENV, "").strip()
    dset = os.environ.get(BQ_DATASET_ENV, "").strip()
    return (proj, dset) if proj and dset else None


class Oracle:
    """A DuckDB session attached to the stores a cell needs to read.

    Every attach is explicit: a cell asks for what it reads, so a missing
    credential fails where it is needed instead of at import time for everyone.
    """

    def __init__(
        self,
        *,
        bigquery: bool = False,
        bq_dataset: str | None = None,
        bq_project: str | None = None,
        mysql: str | None = None,
        postgres: str | None = None,
        mssql: str | None = None,
        mongo: str | None = None,
        gcs: bool = False,
        snowflake: dict | None = None,
        state: str | None = None,
        config: dict | None = None,
    ) -> None:
        self.db = duckdb.connect(config=config or {})
        self.project: str | None = None
        if gcs:
            import subprocess

            token = subprocess.run(
                ["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True
            ).stdout.strip()
            self.db.sql("INSTALL httpfs; LOAD httpfs;")
            self.db.sql(f"CREATE SECRET gcs_adc (TYPE gcs, BEARER_TOKEN '{token}')")
        if mssql:
            self.db.sql("INSTALL mssql FROM community; LOAD mssql;")
            self.db.sql(f"ATTACH '{mssql}' AS ms (TYPE mssql, READ_ONLY)")
        if mongo:
            self.db.sql("INSTALL mongo FROM community; LOAD mongo;")
            self.db.sql(f"ATTACH '{mongo}' AS mg (TYPE mongo, READ_ONLY)")
        if mysql:
            self.db.sql("INSTALL mysql; LOAD mysql;")
            self.db.sql(f"ATTACH '{mysql}' AS my (TYPE mysql, READ_ONLY)")
        if postgres:
            self.db.sql("INSTALL postgres; LOAD postgres;")
            quoted = postgres.replace("'", "''")
            self.db.sql(f"ATTACH '{quoted}' AS pg (TYPE postgres, READ_ONLY)")
        if state:
            # rivet's state DB: a Postgres URL, else a SQLite file path.
            kind = "postgres" if state.startswith("postgres") else "sqlite"
            self.db.sql(f"INSTALL {kind}; LOAD {kind};")
            self.db.sql(f"ATTACH '{state}' AS st (TYPE {kind}, READ_ONLY)")
        if bigquery:
            target = (bq_project, bq_dataset or "") if bq_project is not None else bq_target()
            if target is None or not target[0]:
                raise RuntimeError(
                    f"BigQuery oracle needs {BQ_PROJECT_ENV} and {BQ_DATASET_ENV} — "
                    "call `bq_target()` first and SKIP the cell when it returns None, "
                    "so an absent credential never reads as an empty warehouse"
                )
            self.project = target[0]
            # Community extension: first use downloads it into ~/.duckdb.
            self.db.sql("INSTALL bigquery FROM community; LOAD bigquery;")
            # Scoped to the cell's own dataset: a project-wide attach resolves names by
            # listing EVERY dataset, and one a concurrent cell drops mid-listing fails
            # the query (`Not found: Dataset …`).
            scope = f" dataset={bq_dataset}" if bq_dataset else ""
            self.db.sql(f"ATTACH 'project={self.project}{scope}' AS bq (TYPE bigquery, READ_ONLY)")

        if snowflake:
            import adbc_driver_snowflake

            # The driver is the wheel uv.lock pins, not whatever sits in ~/.duckdb.
            os.environ[SF_DRIVER_ENV] = adbc_driver_snowflake._driver_path()
            login = snowflake_login(str(snowflake["connection"]))
            # Community extension: first use downloads it into ~/.duckdb.
            self.db.sql("INSTALL snowflake FROM community; LOAD snowflake;")
            self.db.sql(snowflake_secret(login, str(snowflake["database"]), str(snowflake["warehouse"])))

    def snowflake(self, sql: str) -> str:
        """A DuckDB relation over Snowflake's own answer to `sql`."""
        return "snowflake_query('" + sql.replace("'", "''") + f"', '{SF_SECRET}')"

    def rows(self, sql: str) -> list[tuple]:
        """Every row of one query, over whatever is attached."""
        return retry(lambda: self.db.sql(sql).fetchall())

    def scalar(self, sql: str):
        """The first column of the first row — the shape a count check wants."""
        row = retry(lambda: self.db.sql(sql).fetchone())
        return None if row is None else row[0]

    def close(self) -> None:
        """Release the session."""
        self.db.close()

    def __enter__(self) -> "Oracle":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
