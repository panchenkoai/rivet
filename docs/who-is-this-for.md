# Who is Rivet for?

A short, honest fit-check.  Rivet is intentionally narrow — the goal is
to do one thing well, not to be the only data tool you need.  This
page exists so you can decide in 60 seconds whether to keep reading,
or whether something else is a better fit for your problem.

If you came here from a search for "Postgres / MySQL / SQL Server / MongoDB → Parquet"
or "extract a big SQL table without an OOM", you are probably in the
right place.

---

## Yes, Rivet is probably a good fit if…

- You need to **dump rows from PostgreSQL, MySQL, or SQL Server (or documents
  from MongoDB) into Parquet or CSV files** — locally, on S3, GCS, or Azure Blob
  Storage.
- The source database is **fragile, production-shared, or behind a
  pooler** (pgBouncer, ProxySQL, MaxScale) and a careless `SELECT *`
  is going to hurt someone.
- You want **resumable extraction** — the job can crash, the network
  can blip, and the next run continues from a checkpoint instead of
  starting over.
- You want a **manifest + `_SUCCESS`** trust contract so a downstream
  loader can decide *exactly* which files belong to a given run.
- You are happy operating Rivet from **cron, Airflow, GitHub Actions,
  Argo, a one-off shell script, or a Kubernetes Job** — anything that
  can invoke a single CLI binary with a YAML config.
- You can write a SQL query.  Rivet does not abstract SQL away; it
  runs the query you give it.

---

## No, Rivet is probably *not* the right tool if…

| You actually need… | Use this instead |
|---|---|
| **Always-on live streaming** — every insert/update/delete pushed continuously into Kafka or Kinesis as it happens | [Debezium](https://debezium.io/), [Estuary](https://estuary.dev/), [Materialize](https://materialize.com/), or your cloud's native CDC (`AWS DMS`, `GCP Datastream`). *(Rivet does capture CDC — inserts/updates/deletes — but to **files**: `mode: cdc`, resumable, per-invocation, not a live stream. See [semantics.md](semantics.md).)* |
| **A SaaS connector marketplace** — pre-built connectors for Salesforce, Stripe, NetSuite, Shopify, Hubspot, etc. | [Airbyte](https://airbyte.com/), [Fivetran](https://www.fivetran.com/), [Stitch](https://www.stitchdata.com/) |
| **A *managed* warehouse loader** — a continuously-managed service that loads *every* warehouse (Redshift, Databricks, …) as one product. *(Rivet **does** load BigQuery / Snowflake — `rivet load`, a discrete command you schedule, not a managed service; [recipe](recipes/snowflake-load.md).)* | [Fivetran](https://www.fivetran.com/), [Airbyte](https://airbyte.com/) (cloud), [dlt](https://dlthub.com/) (self-hosted with destinations), [Sling](https://slingdata.io/) |
| **In-warehouse transformation** — modeling, joins, materializations, lineage | [dbt](https://www.getdbt.com/), [SQLMesh](https://sqlmesh.com/) |
| **A data orchestrator** — DAGs, retries-with-callbacks, schedule UI, lineage graphs | [Airflow](https://airflow.apache.org/), [Dagster](https://dagster.io/), [Prefect](https://www.prefect.io/) |
| **A Kubernetes operator / Helm chart for an extraction platform** | Rivet runs as a single binary in a `Job` or `CronJob`; a heavier platform like [Airbyte on Kubernetes](https://docs.airbyte.com/deploying-airbyte/on-kubernetes-via-helm) is a different architecture |
| **Exactly-once delivery to the destination** — no chance of a duplicate file under any failure mode | Rivet provides at-least-once file delivery + manifest; consumers deduplicate on the warehouse side ([recipe](recipes/idempotent-warehouse-load.md)).  If you need exactly-once at the file layer, use a transactional sink (warehouse `MERGE`, lake table commit). |
| **A data catalog / governance / PII detection layer** | [Amundsen](https://www.amundsen.io/), [DataHub](https://datahubproject.io/), [Collibra](https://www.collibra.com/) |
| **A query engine** that *reads* from Postgres/MySQL and joins with other sources at query time | [DuckDB](https://duckdb.org/) (`postgres_scanner`/`mysql_scanner`), [Trino](https://trino.io/), [ClickHouse](https://clickhouse.com/) |

If you find yourself trying to bend Rivet into one of the above
shapes, stop and pick the tool above instead.  We are not trying to
become any of those, and shoehorning will be painful.

---

## Edge cases — Rivet *can* do this, but read first

- **Very large single-table dumps (100M+ rows).**  Yes, but read
  [`docs/modes/chunked.md`](modes/chunked.md) first — chunked mode
  with the right cursor column is the difference between an export
  that finishes in 20 minutes and one that holds a single SQL
  statement open for 2 hours.

- **Sources with weak or missing primary keys / cursor columns.**
  Rivet has `incremental_cursor_mode: coalesce` for nullable
  primaries (see [composite cursor walkthrough](modes/incremental-coalesce.md))
  and ROW_NUMBER-style dense surrogates for chunking — but these
  surface tradeoffs in `rivet check` (sparse range warnings, global
  sort cost).  Look at the warnings, do not ignore them.

- **Read replicas with replication lag.**  Rivet snapshots a
  point-in-time consistent set of rows via a single transaction; if
  the replica lags, you are dumping the replica's "now", not the
  primary's "now".  Operator's responsibility to choose the right
  endpoint.

- **Sources behind SSH bastions / jump hosts.**  No native SSH tunnel
  support yet (tracked in `rivet_roadmap.md` Epic 13).  Use
  `ssh -L` or [`autossh`](https://www.harding.motd.ca/autossh/) to
  forward the port and point Rivet at `localhost:<forwarded>`.

- **Stateless / ephemeral runners (Kubernetes pods, Lambda, ECS
  tasks).**  Set `RIVET_STATE_URL` to a PostgreSQL state backend so
  cursors and checkpoints survive pod death.  See the
  [README § Stateless deployment](https://github.com/panchenkoai/rivet/blob/main/README.md#stateless-deployment).

---

## Decision shortcut

```text
Need always-on live streaming (into Kafka)?    → Debezium / Estuary
Need CDC captured to files (resumable)?        → Rivet  (mode: cdc)
Need a connector for a non-DB SaaS source?     → Airbyte / Fivetran
Need a managed extract+load product?           → Fivetran / Airbyte Cloud
Load extracted data into BigQuery / Snowflake? → Rivet  (rivet load)
Need a SQL-based transformation framework?     → dbt
Need an orchestrator?                          → Airflow / Dagster
Need to dump PG/MySQL to Parquet/CSV safely?   → Rivet
```

If you stayed on the last line, the
[Getting Started guide](getting-started.md) is ~3 minutes and ends
with one Parquet file you can `arrow read` or `duckdb 'SELECT * FROM
read_parquet(…)'` against.
