# Postgres benchmark — 6 tools × 22 tables

Wall / RSS / output measured via `gtime -v`; DB signals from `pg_stat_database`
+ `pg_stat_bgwriter` + `pg_stat_io` deltas plus `pg_stat_activity` sampler @ 50 ms.
All five non-Rivet tools run with their defaults; Rivet uses `table:` shortcut,
`environment: local`, `mode: chunked` with `chunk_size_memory_mb: 256` and the
auto-PK + work_mem-aware FETCH cap.

## Total wall, CPU, RSS, output (22 tables)

| Tool | Wall (s) | User CPU (s) | Peak RSS | Output | Failures |
|---|---:|---:|---:|---:|---:|
| **rivet** | 194.5 | 32.2 | 443 MB | 2.1GB | 0 |
| **sling** | 191.1 | 132.4 | 6004 MB | 2.8GB | 0 |
| **dlt** | 323.4 | 150.1 | 1370 MB | 2.2GB | 1 |
| **duckdb** | 149.8 | 30.8 | 18910 MB | 2.2GB | 1 |
| **clickhouse** | 145.1 | 44.3 | 1639 MB | 2.2GB | 0 |
| **odbc2parquet** | 187.6 | 70.3 | 29061 MB | 972.2MB | 1 |

## DB-side signals — content_items (2 M × 20 wide cols)

| Tool | Longest single query | Peak active backends | xact_commit | temp_bytes | temp_files |
|---|---:|---:|---:|---:|---:|
| rivet | 0.19s | 1 | 4699 | 0 | 0 |
| sling | 134.05s | 1 | 1929 | 0 | 0 |
| dlt | 1.20s | 1 | 3759 | 3.2GB | 200 |
| duckdb | 70.63s | 12 | 692 | 0 | 0 |
| clickhouse | 132.86s | 1 | 1913 | 0 | 0 |
| odbc2parquet | 136.26s | 1 | 2636 | 0 | 0 |

## Sample SQL templates observed (content_items)

### `rivet`
- `FETCH 142 FROM _rive`
- `FETCH 500 FROM _rive`
- `SELECT COUNT(*) FROM public.content_items`
- `SELECT column_name::text, data_type::text, numeric_precision, numeric_scale FROM`

### `sling`
- `select * from "public"."content_items"`

### `dlt`
- `FETCH FORWARD 10000 FROM "c_10df2cb50_1"`
- `FETCH FORWARD 9999 FROM "c_10df2cb50_1"`

### `duckdb`
- `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;`

### `clickhouse`
- `COPY (SELECT "id", "title", "body", "raw_html", "metadata", "tags", "author_name`

### `odbc2parquet`
- `SELECT * FROM public."content_items"`
