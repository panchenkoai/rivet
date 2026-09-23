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

import duckdb

#: The env vars the release gate already uses (dev/release-oracle/matrix.yaml).
BQ_PROJECT_ENV = "BQ_ORACLE_PROJECT"
BQ_DATASET_ENV = "BQ_ORACLE_DATASET"



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
        mysql: str | None = None,
        postgres: str | None = None,
        mssql: str | None = None,
        mongo: str | None = None,
        gcs: bool = False,
    ) -> None:
        self.db = duckdb.connect()
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
            self.db.sql(f"ATTACH '{postgres}' AS pg (TYPE postgres, READ_ONLY)")
        if bigquery:
            target = bq_target()
            if target is None:
                raise RuntimeError(
                    f"BigQuery oracle needs {BQ_PROJECT_ENV} and {BQ_DATASET_ENV} — "
                    "call `bq_target()` first and SKIP the cell when it returns None, "
                    "so an absent credential never reads as an empty warehouse"
                )
            self.project = target[0]
            # Community extension: first use downloads it into ~/.duckdb.
            self.db.sql("INSTALL bigquery FROM community; LOAD bigquery;")
            self.db.sql(f"ATTACH 'project={self.project}' AS bq (TYPE bigquery, READ_ONLY)")

    def rows(self, sql: str) -> list[tuple]:
        """Every row of one query, over whatever is attached."""
        return self.db.sql(sql).fetchall()

    def scalar(self, sql: str):
        """The first column of the first row — the shape a count check wants."""
        row = self.db.sql(sql).fetchone()
        return None if row is None else row[0]

    def close(self) -> None:
        """Release the session."""
        self.db.close()

    def __enter__(self) -> "Oracle":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
