# MySQL benchmark — 5 tools × 17 tables

`odbc2parquet` is excluded — the MySQL ODBC driver isn't available via `brew` on
macOS arm64. The other five tools are the same wire-protocol classes as the PG
matrix. DB signals from `performance_schema.global_status` + `information_schema.processlist`.

## Total wall, CPU, RSS, output (17 tables)

| Tool | Wall (s) | User CPU (s) | Peak RSS | Output | Failures |
|---|---:|---:|---:|---:|---:|
| **rivet** | 150.7 | 29.9 | 280 MB | 2.2GB | 0 |
| **sling** | 186.5 | 136.3 | 6319 MB | 2.8GB | 0 |
| **dlt** | 338.1 | 235.5 | 1181 MB | 2.2GB | 0 |
| **duckdb** | 292.3 | 107.8 | 23054 MB | 2.2GB | 0 |
| **clickhouse** | 187.3 | 193.7 | 1677 MB | 2.2GB | 0 |

## DB-side signals — content_items (~2 M rows, wide)

| Tool | Longest Q | Peak active | Queries | Innodb_rows_read | Bytes sent |
|---|---:|---:|---:|---:|---:|
| rivet | 9s | 1 | 4494 | 2,056,004 | 21.7GB |
| sling | 137s | 1 | 4152 | 2,055,844 | 21.8GB |
| dlt | 208s | 1 | 7739 | 2,058,499 | 21.8GB |
| duckdb | 231s | 1 | 14065 | 2,063,245 | 2.3GB |
| clickhouse | 174s | 1 | 8999 | 2,059,470 | 21.8GB |

**Note**: MySQL `processlist.time` is whole-second granular. A reading of `0s`
means the query finished in <1 s between samples, not that it was instantaneous.
