# Airflow quickstart: CDC and batch pipelines with Docker Compose

Apache Airflow with rivet and [`airflow-provider-rivet`](https://github.com/panchenkoai/airflow-provider-rivet),
demo MySQL and PostgreSQL sources ready for CDC, MinIO as the bucket and ClickHouse as the warehouse, in one
`docker compose up`. Four pipelines run as they are: CDC and batch, from each source, into ClickHouse. Then this page
writes the commands that add your own database as a fifth.

The files are in the provider's
[`quickstart/`](https://github.com/panchenkoai/airflow-provider-rivet/tree/main/quickstart) folder, one compose file
per Airflow version (2.10.5 and 3.3.2). The full guide, with what runs where and the move to production, is
[docs/quickstart.md](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/quickstart.md).

```text
MySQL / PostgreSQL ──► rivet (an Airflow task) ──► MinIO bucket (Parquet) ──► rivet load ──► ClickHouse
                              └── state: cursors, run journal, load ledger ──► rivet-state (PostgreSQL)
```

**Command builder.** Fill in the form below and every command on this page is rewritten with your values, ready to
copy. On GitHub the form is not rendered: replace the `{{…}}` placeholders by hand.

<div id="af-builder"></div>

---

## 1. Start the stack

```bash
git clone --depth 1 https://github.com/panchenkoai/airflow-provider-rivet.git
cd airflow-provider-rivet/quickstart/{{FOLDER}}
cp ../.env.example .env                  # on Linux: set AIRFLOW_UID to the output of `id -u`
docker compose up -d --build --wait      # then http://localhost:8080, airflow / airflow
DC="docker compose"                      # the commands below run `$DC exec …` in this folder, in bash
```

Switch on the four `rivet_*` DAGs in the UI: their first runs land `mysql_cdc.*`, `mysql_batch.*`, `postgres_cdc.*`
and `postgres_batch.*` in ClickHouse (http://localhost:8123/play, user `loader`, password `rivet_clickhouse_demo`).
The guide's step 3 changes the sources and shows the second run: CDC carries inserts, updates and deletes, batch
carries inserts and updates.

## 2. Your database: grants

Run on your database, as an administrator. The containers must reach its host: a database on your own machine is
`host.docker.internal`.

```sql
{{GRANTS}}
```

## 3. The source Connection

The password is read from the terminal, never typed into a command line:

```bash
{{CONN}}
```

## 4. The pipeline's state, bucket and warehouse

Each pipeline has its own state database, bucket, ClickHouse database and state directory: its cursors and CDC
checkpoint belong to it alone, and two sources with the same table names never share a folder or a table.

```bash
{{STATE}}
{{BUCKET}}
{{WAREHOUSE}}
```

## 5. The config, generated from the live database

`rivet init` runs inside the Airflow container, reads your tables and writes `dags/rivet/{{P}}.yaml`, with its key
recorded in the pipeline's own state:

```bash
{{INIT}}
```

Review the file before the first run: `rivet init` guesses partitions and modes from the tables (see
[the config reference](reference/config.md)).

## 6. The DAG

Add this line to `PIPELINES` in `dags/rivet_quickstart.py`:

```python
{{DAG_LINE}}
```

A config with a `mode: cdc` export becomes a CDC DAG (one stream, then a load per table); any other becomes a batch
DAG (an extract and a load per table). Then:

```bash
{{UNPAUSE}}
```

A DAG unpaused before the scheduler has read its file stays paused: Airflow 2.10 prints `No paused DAGs were found`,
Airflow 3.3 prints the DAG as it was. Repeat until `dags list` shows it `False`.

## 7. Check the warehouse

After the first run of `{{DAG}}`:

```bash
{{CHECK}}
```

Each count should be the source table's row count. `<table>` in ClickHouse is a view with one row per key, the
latest version; a CDC table keeps a deleted key with `__is_deleted = 1`.

## Notes

- **Batch and the key.** A batch config reads each table through a `query:`, and for a `query:` export `rivet run`
  records no key, so the builder writes `pk: [<key>]` into the config instead of leaving `pk: auto`
  ([rivet#679](https://github.com/panchenkoai/rivet/issues/679)).
- **Your own bucket or ClickHouse** gets its own Connection, named in the pipeline's `PIPELINES` line (`storage`,
  `warehouse`). The demo pipelines keep the stack's MinIO and ClickHouse.
- **CeleryExecutor**: pick `airflow-worker` as the service that runs tasks.
- **ClickHouse** loading is a preview: [known limits](recipes/clickhouse-load.md).
- What the builder's commands were proven with: every command for a PostgreSQL CDC pipeline on Airflow 3.3.2 and for
  a MySQL batch pipeline with its own ClickHouse Connection on Airflow 2.10.5 was run as generated against the
  quickstart stack, and the ClickHouse tables were compared with the source. The `s3` (AWS) bucket branch was not run.

<script src="airflow-quickstart.js"></script>
