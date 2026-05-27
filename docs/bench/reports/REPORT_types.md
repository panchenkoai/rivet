# Type-fidelity benchmark

Sibling of [`REPORT_pg.md`](REPORT_pg.md) — same six tools, same
`rivet_type_matrix` / `rivet_type_matrix_full` seed tables — but
the question is _what does the Parquet schema look like after each
tool exports it?_, not how fast / how much RSS. The headline data
point per cell is the Parquet ``physical+logical`` type pair as
reported by pyarrow; the glyph is a classification against the
Rivet reference. See the legend at the bottom.

Reproduce: `docs/bench/harness/type_bench.sh all` then
`docker exec -i rivet-duckdb python - /work/type_bench <
docs/bench/harness/type_diff.py > docs/bench/reports/REPORT_types.md`.

## Headline

Cell counts per source — `✓` includes `✓+` upgrades, `✗*` is any
loss class, `✗!` is whole-table failure.

### PG

| tool | ✓ exact | ✓+ richer | ~ compat | ✗ lossy | ✗! failed |
|------|--------:|----------:|---------:|--------:|----------:|
| rivet | 17 | 2 | 1 | 0 | 0 |
| sling | 4 | 2 | 3 | 0 | 11 |
| duckdb | 8 | 1 | 0 | 0 | 11 |
| clickhouse | 7 | 0 | 4 | 0 | 9 |
| odbc2parquet | 15 | 0 | 2 | 3 | 0 |

### MYSQL

| tool | ✓ exact | ✓+ richer | ~ compat | ✗ lossy | ✗! failed |
|------|--------:|----------:|---------:|--------:|----------:|
| rivet | 19 | 1 | 0 | 0 | 0 |
| sling | 9 | 1 | 9 | 1 | 0 |
| duckdb | 16 | 0 | 4 | 0 | 0 |
| clickhouse | 7 | 0 | 11 | 2 | 0 |
| odbc2parquet | 0 | 0 | 0 | 0 | 20 |

---

## Per-table detail

### PG · `rivet_type_matrix`

| column | source type | rivet | sling | duckdb | clickhouse | odbc2parquet |
|---|---|---|---|---|---|---|
| `id` | `bigint` | ✓ `INT64` | ✓ `INT64+Integer(bit_width=64,signed=True)` | ✓ `INT64+Integer(bit_width=64,signed=True)` | **✗!** | ✓ `INT64` |
| `label` | `text` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | **✗!** | ✓ `BYTE_ARRAY+String` |
| `amount` | `numeric(18,2)` | ✓ `INT64+Decimal(18,2)` | ~dec `FIXED_LEN_BYTE_ARRAY+Decimal(24,6)` | ✓ `INT64+Decimal(18,2)` | **✗!** | ✓ `INT64+Decimal(18,2)` |
| `fee` | `numeric(18,6)` | ✓ `INT64+Decimal(18,6)` | ~dec `FIXED_LEN_BYTE_ARRAY+Decimal(24,6)` | ✓ `INT64+Decimal(18,6)` | **✗!** | ✓ `INT64+Decimal(18,6)` |
| `created_at` | `timestamp` | ✓ `INT64+Timestamp(MICROS,naive)` | ✓ `INT64+Timestamp(MICROS,naive)` | ✓ `INT64+Timestamp(MICROS,naive)` | **✗!** | ✓ `INT64+Timestamp(MICROS,naive)` |
| `created_at_tz` | `timestamptz` | ✓ `INT64+Timestamp(MICROS,UTC)` | ✓ `INT64+Timestamp(MICROS,UTC)` | ✓ `INT64+Timestamp(MICROS,UTC)` | **✗!** | ✗tz `INT64+Timestamp(MICROS,naive)` |
| `raw_bytes` | `bytea` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY` | **✗!** | ✓ `BYTE_ARRAY` |
| `uid` | `uuid` | ✓+ `FIXED_LEN_BYTE_ARRAY+UUID` | ✓+ `FIXED_LEN_BYTE_ARRAY+UUID` | ✓+ `FIXED_LEN_BYTE_ARRAY+UUID` | **✗!** | ✓ `BYTE_ARRAY+String` |
| `attrs` | `jsonb` | ✓+ `BYTE_ARRAY+JSON` | ✓+ `BYTE_ARRAY+JSON` | ✓ `BYTE_ARRAY+String` | **✗!** | ✓ `BYTE_ARRAY+String` |

- **clickhouse**: ✗! — parquet open failed: Parquet file size is 0 bytes

### PG · `rivet_type_matrix_full`

| column | source type | rivet | sling | duckdb | clickhouse | odbc2parquet |
|---|---|---|---|---|---|---|
| `id` | `bigint` | ✓ `INT64` | **✗!** | **✗!** | ✓ `INT64` | ✓ `INT64` |
| `flag` | `boolean` | ✓ `BOOLEAN` | **✗!** | **✗!** | ~ `INT32+Integer(bit_width=8,signed=False)` | ~ `BYTE_ARRAY+String` |
| `int2_col` | `smallint` | ✓ `INT32+Integer(bit_width=16,signed=True)` | **✗!** | **✗!** | ✓ `INT32+Integer(bit_width=16,signed=True)` | ✓ `INT32+Integer(bit_width=16,signed=True)` |
| `int4_col` | `integer` | ✓ `INT32` | **✗!** | **✗!** | ✓ `INT32` | ✓ `INT32+Integer(bit_width=32,signed=True)` |
| `float4_col` | `real` | ✓ `FLOAT` | **✗!** | **✗!** | ✓ `FLOAT` | ✓ `FLOAT` |
| `date_col` | `date` | ✓ `INT32+Date` | **✗!** | **✗!** | ~ `INT32+Integer(bit_width=16,signed=False)` | ✓ `INT32+Date` |
| `time_col` | `time` | ✓ `INT64+Time(MICROS,naive)` | **✗!** | **✗!** | ~ `BYTE_ARRAY+String` | ~ `BYTE_ARRAY+String` |
| `interval_col` | `interval` | ✓ `BYTE_ARRAY+String` | **✗!** | **✗!** | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` |
| `enum_col` | `enum` | ✓ `BYTE_ARRAY+String` | **✗!** | **✗!** | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` |
| `tags` | `text[]` | ~ `LIST<BYTE_ARRAY>+String` | **✗!** | **✗!** | ~ `LIST<BYTE_ARRAY>+String` | ✗list `BYTE_ARRAY+String` |
| `nums` | `integer[]` | ✓ `LIST<INT32>+List` | **✗!** | **✗!** | ✓ `LIST<INT32>+List` | ✗list `BYTE_ARRAY+String` |

- **sling**: ✗! — parquet open failed: Parquet file size is 0 bytes
- **duckdb**: ✗! — parquet open failed: Parquet file size is 0 bytes

### MYSQL · `rivet_type_matrix`

| column | source type | rivet | sling | duckdb | clickhouse | odbc2parquet |
|---|---|---|---|---|---|---|
| `id` | `bigint` | ✓ `INT64` | ✓ `INT64+Integer(bit_width=64,signed=True)` | ✓ `INT64+Integer(bit_width=64,signed=True)` | ✓ `INT64` | **✗!** |
| `label` | `varchar` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | **✗!** |
| `amount` | `decimal(18,2)` | ✓ `INT64+Decimal(18,2)` | ~dec `FIXED_LEN_BYTE_ARRAY+Decimal(24,6)` | ✓ `INT64+Decimal(18,2)` | ~ `BYTE_ARRAY+String` | **✗!** |
| `fee` | `decimal(18,6)` | ✓ `INT64+Decimal(18,6)` | ~dec `FIXED_LEN_BYTE_ARRAY+Decimal(24,6)` | ✓ `INT64+Decimal(18,6)` | ~ `BYTE_ARRAY+String` | **✗!** |
| `created_at_dt` | `datetime` | ✓ `INT64+Timestamp(MICROS,naive)` | ✓ `INT64+Timestamp(MICROS,naive)` | ✓ `INT64+Timestamp(MICROS,naive)` | ✗ `INT32+Integer(bit_width=32,signed=False)` | **✗!** |
| `created_at_ts` | `timestamp` | ✓ `INT64+Timestamp(MICROS,UTC)` | ✗tz `INT64+Timestamp(MICROS,naive)` | ✓ `INT64+Timestamp(MICROS,UTC)` | ✗ `INT32+Integer(bit_width=32,signed=False)` | **✗!** |
| `raw_bytes` | `binary(4)` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY` | ~ `FIXED_LEN_BYTE_ARRAY` | **✗!** |
| `uid` | `varchar(36)` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | **✗!** |
| `extras` | `json` | ✓+ `BYTE_ARRAY+JSON` | ✓+ `BYTE_ARRAY+JSON` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | **✗!** |

- **odbc2parquet**: ✗! — tool not configured for this source (no comparison data point)

### MYSQL · `rivet_type_matrix_full`

| column | source type | rivet | sling | duckdb | clickhouse | odbc2parquet |
|---|---|---|---|---|---|---|
| `id` | `bigint` | ✓ `INT64` | ✓ `INT64+Integer(bit_width=64,signed=True)` | ✓ `INT64+Integer(bit_width=64,signed=True)` | ✓ `INT64` | **✗!** |
| `flag` | `tinyint(1)` | ✓ `BOOLEAN` | ~ `INT32+Integer(bit_width=16,signed=True)` | ✓ `BOOLEAN` | ~ `INT32+Integer(bit_width=8,signed=True)` | **✗!** |
| `bit1_col` | `bit(1)` | ✓ `BOOLEAN` | ~ `BYTE_ARRAY+String` | ✓ `BOOLEAN` | ~ `INT64+Integer(bit_width=64,signed=False)` | **✗!** |
| `bit8_col` | `bit(8)` | ✓ `INT64` | ~ `BYTE_ARRAY+String` | ~ `BYTE_ARRAY` | ✓ `INT64+Integer(bit_width=64,signed=False)` | **✗!** |
| `tiny_col` | `tinyint` | ✓ `INT32+Integer(bit_width=16,signed=True)` | ✓ `INT32+Integer(bit_width=16,signed=True)` | ~ `INT32+Integer(bit_width=8,signed=True)` | ~ `INT32+Integer(bit_width=8,signed=True)` | **✗!** |
| `date_col` | `date` | ✓ `INT32+Date` | ✓ `INT32+Date` | ✓ `INT32+Date` | ~ `INT32+Integer(bit_width=16,signed=False)` | **✗!** |
| `time_col` | `time(6)` | ✓ `INT64+Time(MICROS,naive)` | ✓ `INT64+Time(MICROS,naive)` | ~ `BYTE_ARRAY+String` | ~ `BYTE_ARRAY+String` | **✗!** |
| `year_col` | `year` | ✓ `INT32+Integer(bit_width=16,signed=True)` | ~ `BYTE_ARRAY+String` | ~ `INT32+Integer(bit_width=32,signed=True)` | ~ `BYTE_ARRAY+String` | **✗!** |
| `enum_col` | `enum` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY+String` | **✗!** |
| `varbinary_col` | `varbinary(4)` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | **✗!** |
| `blob_col` | `blob` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | ✓ `BYTE_ARRAY` | ~ `BYTE_ARRAY+String` | **✗!** |

- **odbc2parquet**: ✗! — tool not configured for this source (no comparison data point)

---

### Legend

| Glyph | Meaning |
|-------|---------|
| `✓` | Exact match against the Rivet reference (same physical + logical type) |
| `✓+` | Tool emits **richer** Parquet logical type than Rivet (e.g. native `UUID` / `JSON` logical type vs `String`) |
| `~` | Different physical/logical but values preserved |
| `~dec` | Decimal precision/scale coarsened (e.g. `Decimal(18,2)` → `Decimal(24,6)`) — values fit but the column contract is widened |
| `✗` | Silent lossy degradation (e.g. `Decimal(p,s)` → `DOUBLE` loses precision past 2⁵³) |
| `✗tz` | Timezone information dropped from a tz-aware timestamp |
| `✗list` | LIST structure flattened to a string blob — array semantics lost |
| `✗!` | Tool failed to produce a Parquet at all — the failure mode itself is the comparison |
| `.` | Tool not configured for this source/table (no data point) |
