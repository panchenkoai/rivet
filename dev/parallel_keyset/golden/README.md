# Parallel-keyset sparse-table GOLDEN — cross-engine, fixed counts

A deterministic golden for the **keyset-parallel** scenario on a WIDE, DEEP,
very-SPARSE-key table. Runs on Postgres, MySQL, SQL Server — the SQL keyset
engines (Mongo's `parallel: N` is the separate `_id`-range path, not SQL keyset,
so it is out of this golden). **Heavy (10M rows) — seed + run on the devbox, not
in the local unit/live suite.**

## Why sparse

The key is `id = n × 997` for `n` in `1..10_000_000` → `id` spans ~9.97 × 10⁹ with
10M actual rows: **span_per_row ≈ 997**. Range chunking would carve the key SPAN
(`(max-min)/chunk_size`) into ~20K near-empty `BETWEEN` windows — the documented
sparse-key footgun. Keyset pages by ROWS on the unique index, so it is immune: the
file count depends only on `rows / chunk_size / parallel`, never on the key span.
That immunity is exactly what this golden pins.

## The golden (parallel=4, chunk_size=500_000)

**Empirically measured (validated on a 1M-row uniform table, same 4×-range ÷5
ratio), NOT theorised — the file count is 21, not the naive 20.** The N-1
boundaries are sampled at ROW offsets `total×i/N` (0-indexed), and range 0 is the
INCLUSIVE `(-∞, b1]`, so the FIRST range holds `total/N + 1` rows while the middle
ranges hold `total/N` and the last holds `total/N - 1`. At `chunk_size = 500_000`
the first range's `2_500_001` rows tip into a **6th** part (5 full pages + 1 row);
the other three ranges write 5 each → **6 / 5 / 5 / 5 = 21 files**. Deterministic
(the OFFSET boundary is fixed) and engine-agnostic (OFFSET over a unique key
returns the same id on PG/MySQL/MSSQL; keyset pages by row).

| metric | golden value |
|---|---|
| rows (every engine) | **10,000,000** |
| distinct id (every engine) | **10,000,000** (no boundary drop/dup) |
| part files (every engine) | **21** (distribution 6 / 5 / 5 / 5) |
| per-range rows | 2,500,001 / 2,500,000 / 2,500,000 / 2,499,999 |

A run that reads ≠ 10,000,000 rows, ≠ 10,000,000 distinct ids, or writes ≠ 21
files is RED — a boundary drop/dup, a lost range, or a part-name collision.

### BIGINT UNSIGNED variant (MySQL only)

`fixture_mysql_unsigned.sql` seeds `keyset_sparse_unsigned` with the key ENTIRELY
above i64::MAX (`id = 10^19 + n×10^8`) — the regime that broke unsigned keyset in
the field (u64 keys mishandled as signed). Same golden (10M rows / 21 files):
keyset pages by row, so the magnitude doesn't change the counts — but the boundary
LITERALS and the paging cursor must round-trip as u64, which this pins. PG and SQL
Server have no unsigned BIGINT, so this variant is MySQL-only.

## Files

- `fixture_postgres.sql` / `fixture_mysql.sql` / `fixture_mssql.sql` — create +
  deterministically fill `keyset_sparse` (10M rows, wide, `id = n×997`). Same rows
  on every engine (all columns derived from `n`), types mapped per dialect.
- `config.yaml` — the rivet keyset-parallel config (`chunk_by_key: id`,
  `parallel: 4`, `chunk_size: 500000`), destination local parquet.
- `verify.sh` — devbox runner: seed → `rivet run` → assert rows == 10,000,000 AND
  parts == 20 (reads the destination parquet + counts files), per engine.

## Run on the devbox

```bash
# seed each engine (heavy — minutes; PG fastest, MSSQL slowest)
psql "$PG_URL"     -f fixture_postgres.sql
mysql  ... < fixture_mysql.sql
sqlcmd ... -i fixture_mssql.sql
# then, per engine, point config.yaml at the engine URL and:
./verify.sh postgres   # → PASS: 10,000,000 rows / 20 files
```

Wire into `dev/release-oracle/matrix.yaml` as a scenario
`keyset_parallel_sparse_golden` (applies: [postgres, mysql, mssql]) once the counts
are blessed live on the devbox — keep it an OPT-IN heavy stage so the per-version
oracle sweep is not slowed by a 10M seed.
