# Type-fidelity benchmark — BigQuery autoload

Sibling of [`REPORT_types.md`](REPORT_types.md). Same canonical
PG / MySQL matrix tables, same Rivet-produced Parquet — but the
downstream reader is `bq load --autodetect --source_format=PARQUET
--parquet_enable_list_inference --parquet_enum_as_string`.
Each cell shows the type BigQuery picked plus a classification
glyph against the ideal BQ representation. See the legend at the
bottom and the *Findings* section for BigQuery-specific behaviour.

Reproduce:

```bash
docs/bench/harness/type_bench.sh all          # refresh parquet
docs/bench/harness/type_bench_bq.sh all       # load each into BQ
python3 docs/bench/harness/type_diff_bq.py \
    tests/.live-tmp/type_bench > docs/bench/reports/REPORT_types_bigquery.md
```

## Headline

| source | table | ✓ exact | ~ compat | ✗json | ✗tz | ✗! fail |
|--------|-------|--------:|---------:|------:|----:|--------:|
| pg | `rivet_type_matrix` | 8 | 0 | 1 | 0 | 0 |
| pg | `rivet_type_matrix_full` | 11 | 0 | 0 | 0 | 0 |
| mysql | `rivet_type_matrix` | 8 | 0 | 1 | 0 | 0 |
| mysql | `rivet_type_matrix_full` | 11 | 0 | 0 | 0 | 0 |

---

## Per-table detail

### PG · `rivet_type_matrix`

| column | source type | BigQuery autodetect | ideal | glyph |
|--------|-------------|---------------------|-------|-------|
| `id` | `bigint` | `INTEGER` | `INTEGER` | ✓ |
| `label` | `text` | `STRING` | `STRING` | ✓ |
| `amount` | `numeric(18,2)` | `NUMERIC` | `NUMERIC` | ✓ |
| `fee` | `numeric(18,6)` | `NUMERIC` | `NUMERIC` | ✓ |
| `created_at` | `timestamp` | `TIMESTAMP` | `TIMESTAMP` | ✓ |
| `created_at_tz` | `timestamptz` | `TIMESTAMP` | `TIMESTAMP` | ✓ |
| `raw_bytes` | `bytea` | `BYTES` | `BYTES` | ✓ |
| `uid` | `uuid` | `BYTES` | `BYTES` | ✓ |
| `attrs` | `jsonb` | `BYTES` | `JSON` | ✗json |

Aggregates after round-trip:

- **n**: `4`
- **s_amount**: `99999999990024`
- **s_fee**: `10000003`

### PG · `rivet_type_matrix_full`

| column | source type | BigQuery autodetect | ideal | glyph |
|--------|-------------|---------------------|-------|-------|
| `id` | `bigint` | `INTEGER` | `INTEGER` | ✓ |
| `flag` | `boolean` | `BOOLEAN` | `BOOLEAN` | ✓ |
| `int2_col` | `smallint` | `INTEGER` | `INTEGER` | ✓ |
| `int4_col` | `integer` | `INTEGER` | `INTEGER` | ✓ |
| `float4_col` | `real` | `FLOAT` | `FLOAT` | ✓ |
| `date_col` | `date` | `DATE` | `DATE` | ✓ |
| `time_col` | `time` | `TIME` | `TIME` | ✓ |
| `interval_col` | `interval` | `STRING` | `STRING` | ✓ |
| `enum_col` | `enum` | `STRING` | `STRING` | ✓ |
| `tags` | `text[]` | `RECORD` | `RECORD` | ✓ |
| `nums` | `integer[]` | `RECORD` | `RECORD` | ✓ |

Aggregates after round-trip:

- **n**: `3`

### MYSQL · `rivet_type_matrix`

| column | source type | BigQuery autodetect | ideal | glyph |
|--------|-------------|---------------------|-------|-------|
| `id` | `bigint` | `INTEGER` | `INTEGER` | ✓ |
| `label` | `varchar` | `STRING` | `STRING` | ✓ |
| `amount` | `decimal(18,2)` | `NUMERIC` | `NUMERIC` | ✓ |
| `fee` | `decimal(18,6)` | `NUMERIC` | `NUMERIC` | ✓ |
| `created_at_dt` | `datetime` | `TIMESTAMP` | `TIMESTAMP` | ✓ |
| `created_at_ts` | `timestamp` | `TIMESTAMP` | `TIMESTAMP` | ✓ |
| `raw_bytes` | `binary(4)` | `BYTES` | `BYTES` | ✓ |
| `uid` | `varchar(36)` | `STRING` | `STRING` | ✓ |
| `extras` | `json` | `BYTES` | `JSON` | ✗json |

Aggregates after round-trip:

- **n**: `4`
- **s_amount**: `99999999990024`
- **s_fee**: `10000003`

### MYSQL · `rivet_type_matrix_full`

| column | source type | BigQuery autodetect | ideal | glyph |
|--------|-------------|---------------------|-------|-------|
| `id` | `bigint` | `INTEGER` | `INTEGER` | ✓ |
| `flag` | `tinyint(1)` | `BOOLEAN` | `BOOLEAN` | ✓ |
| `bit1_col` | `bit(1)` | `BOOLEAN` | `BOOLEAN` | ✓ |
| `bit8_col` | `bit(8)` | `INTEGER` | `INTEGER` | ✓ |
| `tiny_col` | `tinyint` | `INTEGER` | `INTEGER` | ✓ |
| `date_col` | `date` | `DATE` | `DATE` | ✓ |
| `time_col` | `time(6)` | `TIME` | `TIME` | ✓ |
| `year_col` | `year` | `INTEGER` | `INTEGER` | ✓ |
| `enum_col` | `enum` | `STRING` | `STRING` | ✓ |
| `varbinary_col` | `varbinary(4)` | `BYTES` | `BYTES` | ✓ |
| `blob_col` | `blob` | `BYTES` | `BYTES` | ✓ |

Aggregates after round-trip:

- **n**: `3`

---

## Findings — BigQuery autoload quirks

1. **`LogicalType::Json` → `BYTES`, not `JSON`.** BigQuery's
   Parquet autoloader does not promote a Parquet `LogicalType::Json`
   column into a native BQ `JSON` field. The bytes on disk are
   the same valid JSON Rivet emits (`pyarrow` confirms this in
   `pyarrow_validates_*` tests); BigQuery just falls back to
   `BYTES` instead of `STRING` / `JSON` for this logical type.
   To materialise as native `JSON`, supply an explicit schema:

   ```bash
   bq load --replace --source_format=PARQUET \
       --schema='id:INTEGER,attrs:JSON,...' \
       dataset.table file.parquet
   ```

2. **`LogicalType::Uuid` → `BYTES`.** Expected — BigQuery has no
   native UUID type. The 16-byte payload is the canonical compact
   form; cast in SQL when needed (`TO_HEX(uid)` for display).

3. **TIMESTAMP / TIMESTAMPTZ both → `TIMESTAMP`.** BigQuery's
   `TIMESTAMP` is always UTC, so the tz/naive distinction is
   collapsed at autoload. Wall-clock values round-trip exactly.

4. **Integer widths collapse to `INTEGER`.** BigQuery has one
   integer type (64-bit). `INT16`/`INT32`/`UINT64` all become
   `INTEGER` — fine for value preservation, but precision and
   storage class are normalised.

5. **PG arrays → `RECORD (REPEATED)`.** Inner element type is
   preserved through the BQ schema; `tags TEXT[]` lands as
   `RECORD<STRING REPEATED>`.

6. **Enum → STRING (with `--parquet_enum_as_string`).** Without
   the flag BQ would surface our enum logical type as the raw
   enum index, which is rarely what operators want.

---

### Legend

| Glyph | Meaning |
|-------|---------|
| `✓` | BigQuery autoload picked the ideal BQ type for the semantic |
| `~` | Different name but value-preserving (e.g. INT16 → `INTEGER`) |
| `✗json` | Parquet `LogicalType::Json` did not lift to BQ native `JSON`; landed as `BYTES`/`STRING`. Value bytes are valid JSON — needs explicit schema for native `JSON` materialisation. |
| `✗tz` | tz-aware timestamp lost timezone information (BQ collapses) |
| `✗!` | Column missing from BQ schema — `bq load` failed for this cell |
