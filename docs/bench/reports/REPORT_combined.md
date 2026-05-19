# Combined report — Postgres vs MySQL across 5 tools

Same workload class on both databases (the seed fixtures are aligned). Compares the
**same five tools** that run on both: rivet, sling, dlt, duckdb, clickhouse.
(`odbc2parquet` only on PG — see MySQL report for the methodology note.)

## Total wall + RSS, by tool, side by side

| Tool | Wall PG → MySQL | RSS PG → MySQL | Longest Q on content_items |
|---|---:|---:|---:|
| **rivet** | 194.5s → 150.7s | 443 MB → 280 MB | 0.19s → 9s |
| **sling** | 191.1s → 186.5s | 6004 MB → 6319 MB | 134.05s → 137s |
| **dlt** | 323.4s → 338.1s | 1370 MB → 1181 MB | 1.20s → 208s |
| **duckdb** | 149.8s → 292.3s | 18910 MB → 23054 MB | 70.63s → 231s |
| **clickhouse** | 145.1s → 187.3s | 1639 MB → 1677 MB | 132.86s → 174s |

## Headline observations

1. **Rivet wins both databases on the source-friendliness axes** (longest single
   query, peak concurrent backends, CPU). The chunked + auto-PK + memory-budget
   strategy is source-neutral; only the wire driver differs.
2. **DuckDB's parallel postgres_scanner is PG-specific**. Its mysql_scanner
   counterpart is single-threaded, so the multi-minute wins seen on PG don't
   transfer to MySQL.
3. **Sling and ClickHouse fire one long query per export on both DBs** —
   2-minute single-statement holds that would trip `statement_timeout` or
   `max_execution_time` on a typical production policy.
