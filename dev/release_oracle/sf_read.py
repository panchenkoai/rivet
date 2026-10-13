"""Rows of one Snowflake query as JSON, read through DuckDB's `snowflake` extension.

The live cells' reader of the warehouse: rivet's loader drives `snow`, this reads through an
ADBC driver, so a cell that compares the two is not comparing a tool with itself.
stdin: `{"connection", "database", "warehouse", "sql"}`; stdout: a JSON list of rows, each a
list in SELECT order (a number Snowflake returns as NUMBER is its exact text).
"""

from __future__ import annotations

import json
import sys

from .duck import Oracle


def plain(v):
    """A JSON value: numbers, text and NULL as they are, anything else as its text."""
    return v if v is None or isinstance(v, (bool, int, float, str)) else str(v)


def main() -> None:
    spec = json.load(sys.stdin)
    ora = Oracle(snowflake=spec)
    try:
        rows = ora.rows(f"SELECT * FROM {ora.snowflake(spec['sql'])}")
    finally:
        ora.close()
    json.dump([[plain(v) for v in r] for r in rows], sys.stdout)


if __name__ == "__main__":
    main()
