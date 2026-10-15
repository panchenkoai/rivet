"""The cloud env every live warehouse cell reads, filled from dev/stand/registry.yaml.

    python -m dev.pytools.warehouse_env -- CMD ...   fill the env, say which, then exec CMD

A variable the shell already set wins. BigQuery/GCS are filled when a Google credential is at
hand, Snowflake when the `snow` connection logs in and sees the schema; otherwise the line says
SKIP and the cells report that nothing was verified. The release gate and `make test-live` both
fill it here, so a full live run outside the gate checks the warehouse cells as the gate does.
"""

from __future__ import annotations

import json
import os
import re
import sys

from . import registry, shell


def bigquery() -> str:
    """Fill the BigQuery/GCS env where empty when a Google credential is at hand; the line saying which."""
    if not shell.run(["gcloud", "auth", "print-access-token"], timeout=120).ok:
        return ("BigQuery/GCS legs SKIP: no Google credential (`gcloud auth print-access-token` failed: "
                "`gcloud auth login` and `gcloud auth application-default login`)")
    project, bucket, location = registry.warehouse()
    for var, value in (("BQ_ORACLE_PROJECT", project), ("BQ_ORACLE_BUCKET", bucket),
                       ("BQ_ORACLE_DATASET", registry.bq_tmp("gate"))):
        os.environ[var] = os.environ.get(var) or value
    for var, value in (("BIGQUERY_TEST_PROJECT", os.environ["BQ_ORACLE_PROJECT"]),
                       ("RIVET_TEST_GCS_BUCKET", os.environ["BQ_ORACLE_BUCKET"]), ("BIGQUERY_TEST_LOCATION", location)):
        os.environ[var] = os.environ.get(var) or value
    return f"warehouse: BigQuery {os.environ['BQ_ORACLE_PROJECT']} ({location}), GCS bucket {os.environ['BQ_ORACLE_BUCKET']}"


def snowflake() -> str:
    """Fill the Snowflake env where empty when the `snow` connection logs in and sees the schema; the line saying which."""
    sf = registry.snowflake()
    conn = os.environ.get("RIVET_SF_CONNECTION") or sf["connection"]
    database = os.environ.get("RIVET_SF_DATABASE") or sf["database"]
    schema = os.environ.get("RIVET_SF_SCHEMA") or sf["schema"]
    probe = shell.run(["snow", "sql", "-c", conn, "--format", "json", "-q",
                       f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE {database}"], timeout=120)
    if not probe.ok:
        why = " ".join(re.sub("[─-╿]", " ", probe.stderr or probe.stdout or "").split())[:200]
        return (f"Snowflake legs SKIP: the `snow` connection `{conn}` did not log in "
                f"(`snow connection test -c {conn}`): {why}")
    try:
        found = json.loads(probe.stdout or "[]")
    except json.JSONDecodeError:
        found = []
    if not found:
        return f"Snowflake legs SKIP: no schema {database}.{schema} for the `snow` connection `{conn}`"
    for var, value in (("RIVET_SF_CONNECTION", conn), ("RIVET_SF_WAREHOUSE", sf["warehouse"]),
                       ("RIVET_SF_DATABASE", database), ("RIVET_SF_SCHEMA", schema),
                       ("RIVET_SF_STORAGE_INTEGRATION", sf["storage_integration"]),
                       ("SNOWFLAKE_TEST_CONNECTION", conn), ("SNOWFLAKE_TEST_DATABASE", database),
                       ("SNOWFLAKE_TEST_SCHEMA", schema), ("RIVET_SF_TEST_WAREHOUSE", sf["warehouse"]),
                       ("RIVET_SF_TEST_DATABASE", database), ("RIVET_SF_TEST_SCHEMA", schema)):
        os.environ[var] = os.environ.get(var) or value
    return (f"warehouse: Snowflake {os.environ['RIVET_SF_DATABASE']}.{os.environ['RIVET_SF_SCHEMA']} "
            f"(connection {os.environ['RIVET_SF_CONNECTION']}, warehouse {os.environ['RIVET_SF_WAREHOUSE']})")


def main(argv: list[str]) -> int:
    """Fill the env, print both lines, exec the command after `--`."""
    cmd = argv[1:] if argv[:1] == ["--"] else argv
    if not cmd:
        print(__doc__, file=sys.stderr)
        return 2
    for line in (bigquery(), snowflake()):
        print(f"  {line}", file=sys.stderr, flush=True)
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
