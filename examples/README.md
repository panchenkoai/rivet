# `examples/` — CLI sample configs, NOT Rust crate examples

> Rivet is a CLI product (the `rivet` binary), not a Rust library you
> would call from your own crate.  Despite the `examples/` directory
> name, the files below are **not** `cargo run --example` targets:
> they are sample `rivet.yaml` configs you copy into your own
> repository and run with `rivet run -c <path>`.

A future release may move them under `sample-configs/` to honor the
Rust convention; for now we keep the existing path to preserve the
URLs already linked from blog posts, the main README, and external
documentation.

## Pick a starting point

| File | Source | Mode | Destination |
|---|---|---|---|
| [`rivet.yaml`](rivet.yaml) | PostgreSQL | full | local |
| [`pg_full_local.yaml`](pg_full_local.yaml) | PostgreSQL | full | local |
| [`pg_incremental_local.yaml`](pg_incremental_local.yaml) | PostgreSQL | incremental | local |
| [`pg_chunked_s3.yaml`](pg_chunked_s3.yaml) | PostgreSQL | chunked | S3 |
| [`pg_date_chunked_local.yaml`](pg_date_chunked_local.yaml) | PostgreSQL | chunked-by-days | local |
| [`pg_time_window_gcs.yaml`](pg_time_window_gcs.yaml) | PostgreSQL | time-window | GCS |
| [`pg_partitioned_gcs.yaml`](pg_partitioned_gcs.yaml) | PostgreSQL | full + `partition_by` (Hive `col=value/`) | GCS |
| [`pg_full_azure_sas.yaml`](pg_full_azure_sas.yaml) | PostgreSQL | full | Azure Blob (SAS token) |
| [`mysql_full_local.yaml`](mysql_full_local.yaml) | MySQL | full | local |
| [`mysql_incremental_local.yaml`](mysql_incremental_local.yaml) | MySQL | incremental | local |
| [`mysql_full_azure_sas.yaml`](mysql_full_azure_sas.yaml) | MySQL | full | Azure Blob (SAS token) |

Every example carries a header comment explaining the use case and
the exact command to run it.

## Editor validation

Every config can be statically validated by your editor's YAML
Language Server (VS Code, Neovim, Helix) by adding a `$schema`
directive to the top of the file.  The example
[`pg_full_local.yaml`](pg_full_local.yaml) ships with the header
already in place:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/panchenkoai/rivet/main/schemas/latest/rivet.schema.json
```

Or generate a pinned per-version schema from the binary you have
installed:

```sh
rivet schema config > rivet.schema.json
# then in rivet.yaml:
#   # yaml-language-server: $schema=./rivet.schema.json
```

See [`schemas/`](../schemas/) for the in-tree schema artifact.

## Runnable end-to-end demos

If you want a turn-key Postgres-or-MySQL-in-Docker pipeline with
seed data, head to [`../demo/`](../demo/) instead:

| File | Purpose |
|---|---|
| [`../demo/setup_demo_tables.sql`](../demo/setup_demo_tables.sql) | Seed a PostgreSQL `customers`/`orders` schema. |
| [`../demo/setup_demo_tables_mysql.sql`](../demo/setup_demo_tables_mysql.sql) | Same for MySQL. |
| [`../demo/demo_pipeline.yaml`](../demo/demo_pipeline.yaml) | Multi-export Rivet config that exercises the seeded schema. |
| [`../demo/demo_pipeline_mysql.yaml`](../demo/demo_pipeline_mysql.yaml) | MySQL variant. |

`docker compose up` (from the repo root) brings up the matching
backing databases via [`docker-compose.yaml`](../docker-compose.yaml).
