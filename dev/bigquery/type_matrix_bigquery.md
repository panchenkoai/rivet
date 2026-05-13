# Load `rivet_type_matrix` Parquet into BigQuery & verify types

This complements the docker-compose tables in `dev/postgres/init.sql`, `dev/mysql/init.sql`, and the exporters `dev/workbench/pg_type_matrix.yaml`, `dev/workbench/mysql_type_matrix.yaml`.

## 1. Create data (local Parquet)

From the repository root:

**PostgreSQL**

```bash
mkdir -p dev/output/type_matrix/pg
rivet run --config dev/workbench/pg_type_matrix.yaml --export type_matrix_pg
PG_PARQUET=$(ls -t dev/output/type_matrix/pg/*.parquet | head -1)
echo "$PG_PARQUET"
```

**MySQL** (Parquet columns match PG via aliases in `dev/workbench/mysql_type_matrix.yaml`)

```bash
mkdir -p dev/output/type_matrix/mysql
rivet run --config dev/workbench/mysql_type_matrix.yaml --export type_matrix_mysql
MYSQL_PARQUET=$(ls -t dev/output/type_matrix/mysql/*.parquet | head -1)
echo "$MYSQL_PARQUET"
```

Apply the table to an **older** Postgres volume if needed — the postgres image only mounts `dev/postgres/init.sql`, so replay SQL **from the repo root via stdin**:

```bash
docker compose exec -T postgres psql -U rivet -d rivet -v ON_ERROR_STOP=1 \
  < dev/sql/rivet_type_matrix_postgres.sql
```

MySQL (same idea — init lives only inside the image):

```bash
docker compose exec -T mysql mysql -urivet -privet rivet \
  < dev/sql/rivet_type_matrix_mysql.sql
```

Fresh `docker compose up` picks the table from updated `init.sql` automatically.

## 2. Prerequisites for BigQuery

- [Google Cloud SDK](https://cloud.google.com/sdk) with `bq`
- A project and billing (or sandbox) enabled
- Authentication: `gcloud auth login` and `gcloud config set project myproj`

Pick a dataset (create once):

```bash
export GCP_PROJECT="myproj"
export BQ_DATASET="rivet_type_lab"
export BQ_LOCATION="EU"   # or US, etc.

bq mk --dataset --location="${BQ_LOCATION}" "${GCP_PROJECT}:${BQ_DATASET}" 2>/dev/null || true
```

## 3. Load Parquet into BigQuery

`--autodetect` maps Arrow DECIMAL → `NUMERIC`, timestamps → `TIMESTAMP`, `BYTES` → `BYTES`, strings → `STRING`. Use `--replace` to truncate on reload.

Use **two BigQuery table names** if you load both engines side by side, e.g. **`type_matrix_pg`** and **`type_matrix_mysql`**.

### 3a. Load Parquet from Postgres

`dev/workbench/pg_type_matrix.yaml` → column names `created_at`, `created_at_tz`, `attrs`.

```bash
export TABLE="type_matrix_pg"
export PG_PARQUET=$(ls -t dev/output/type_matrix/pg/*.parquet | head -1)

bq load \
  --project_id="${GCP_PROJECT}" \
  --source_format=PARQUET \
  --replace \
  --autodetect \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}" \
  "${PG_PARQUET}"
```

**Do not** append a second local `.parquet` path (shell glob with two files makes `bq` treat the second file as a JSON schema — see §7). One file:

```bash
export TABLE="type_matrix_pg"
PQ=$(ls -t dev/output/type_matrix/pg/*.parquet | head -1)
bq load --project_id="${GCP_PROJECT}" --source_format=PARQUET --replace --autodetect \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}" \
  "${PQ}"
```

### 3b. Load Parquet from MySQL

`dev/workbench/mysql_type_matrix.yaml` aliases `created_at_dt` / `created_at_ts` / `extras` to **`created_at`** / **`created_at_tz`** / **`attrs`**, so **the same BigQuery SQL** (§4–6) works after you point `${TABLE}` at the MySQL-loaded table.

```bash
export TABLE="type_matrix_mysql"
export MYSQL_PARQUET=$(ls -t dev/output/type_matrix/mysql/*.parquet | head -1)

bq load \
  --project_id="${GCP_PROJECT}" \
  --source_format=PARQUET \
  --replace \
  --autodetect \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}" \
  "${MYSQL_PARQUET}"
```

One-file pattern (same pitfall as Postgres if `*.parquet` expands):

```bash
export TABLE="type_matrix_mysql"
MQ=$(ls -t dev/output/type_matrix/mysql/*.parquet | head -1)
bq load --project_id="${GCP_PROJECT}" --source_format=PARQUET --replace --autodetect \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}" \
  "${MQ}"
```

### 3c. Many parts / GCS

Multiple local parts: loop one file per load, or **`gsutil cp`** then load with `gs://…/*.parquet`:

```bash
# Example: Postgres shards to GCS (repeat pattern for mysql/ subfolder)
gsutil cp dev/output/type_matrix/pg/*.parquet "gs://${GCS_BUCKET}/type_matrix/pg/"
bq load --project_id="${GCP_PROJECT}" --source_format=PARQUET --replace --autodetect \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}" \
  "gs://${GCS_BUCKET}/type_matrix/pg/*.parquet"
```

## 4. Inspect schema (did complex types land correctly?)

Set **`export TABLE=type_matrix_pg`** or **`export TABLE=type_matrix_mysql`** to match which Parquet you loaded in §3.

Pretty JSON schema:

```bash
bq show --project_id="${GCP_PROJECT}" --schema --format=prettyjson \
  "${GCP_PROJECT}:${BQ_DATASET}.${TABLE}"
```

Column list with types:

```bash
bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false --format=pretty \
  "SELECT column_name, data_type
   FROM \`${GCP_PROJECT}.${BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS\`
   WHERE table_name = '${TABLE}'
   ORDER BY ordinal_position"
```

Expectations (typical Arrow → BQ):

| Parquet / Rivet habit | BigQuery (`--autodetect`) |
|------------------------|---------------------------|
| `DECIMAL128(18,2)` amount | `NUMERIC` |
| `DECIMAL128(18,6)` fee | `NUMERIC` |
| TIMESTAMP without TZ (`created_at`) | `TIMESTAMP` (interpretation follows Parquet) |
| TIMESTAMP with UTC hint (`created_at_tz`) | `TIMESTAMP` |
| Binary (`raw_bytes`) | `BYTES` |
| Utf8 UUID / Json (`uid`, `attrs`) | `STRING` |

`attrs` stays **STRING**. To validate JSON text:

```bash
bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false \
  "SELECT id, SAFE.PARSE_JSON(attrs) AS attrs_json
   FROM \`${GCP_PROJECT}.${BQ_DATASET}.${TABLE}\`
   ORDER BY id"
```

## 5. Financial checksums (`amount`, `fee`)

Seed rows in **`dev/postgres/init.sql`** and **`dev/mysql/init.sql`** (`rivet_type_matrix`) are the same four tuples. **`SUM(amount)`** and **`SUM(fee)`** invariants hold for **both** Parquet exports (`type_matrix_pg` and `type_matrix_mysql`) — same golden numbers as [`live_type_golden.rs`](../../tests/live_type_golden.rs) (`golden_decimal_*`, `mysql_golden_decimal_*`).

| `id` | `amount`   | `fee`      |
|------|------------|------------|
| 1    | 0.10       | 0.000001   |
| 2    | 0.20       | 0.000002   |
| 3    | 999999999999.99 | 10.123456 |
| 4    | -100.05    | -0.123456  |

Expected totals (**exact**, do not approximate with `FLOAT64` for pass/fail):

- **`SUM(amount)`** = **`999999999900.24`**
- **`SUM(fee)`** = **`10.000003`**

(Optional) warn-only line: `SUM(SAFE_CAST(fee AS FLOAT64))` vs exact `SUM(fee)` shows why float must not judge money.

**One-shot check** — prints actual sums next to literals and booleans (same SQL for PG or MySQL table; only `${TABLE}` differs):

```bash
bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false \
  "SELECT
     SUM(amount) AS sum_amount,
     NUMERIC '999999999900.24' AS expected_sum_amount,
     SUM(amount) = NUMERIC '999999999900.24' AS amount_ok,
     SUM(fee) AS sum_fee,
     NUMERIC '10.000003' AS expected_sum_fee,
     SUM(fee) = NUMERIC '10.000003' AS fee_ok
   FROM \`${GCP_PROJECT}.${BQ_DATASET}.${TABLE}\`"
```

Row-level anchors (scaled view used in Rust golden tests):

- **`amount`** scale \(10^2\): \(10,\ 20,\ 99999999999999,\ -10005\) → scaled sum **`99999999990024`**.
- **`fee`** scale \(10^6\): \(1,\ 2,\ 10123456,\ -123456\) → scaled sum **`10000003`**.

Same-table probe (optional):

```bash
bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false \
  "SELECT id, amount, fee
   FROM \`${GCP_PROJECT}.${BQ_DATASET}.${TABLE}\`
   ORDER BY id"
```

## 6. Timestamp / BYTES probes

```bash
bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false \
  "SELECT id, created_at, created_at_tz FROM \`${GCP_PROJECT}.${BQ_DATASET}.${TABLE}\` ORDER BY id"

bq query --project_id="${GCP_PROJECT}" --use_legacy_sql=false \
  "SELECT id, TO_HEX(raw_bytes) AS raw_hex FROM \`${GCP_PROJECT}.${BQ_DATASET}.${TABLE}\` ORDER BY id"
```

Row `id = 2`: for **Postgres**, `created_at` (naive) vs `created_at_tz` (instant) can differ when the seed uses a non‑UTC offset. **MySQL** demo uses the same wall‑clock string for `DATETIME` and `TIMESTAMP(6)` in compose init — values should still match exactly in BigQuery for this seed; if your session / server TZ differs, validate money sums (§5) first, then compare timestamps to source.

## 7. Troubleshooting BigQuery schemas

**MySQL-only: `raw_bytes` loads as STRING and looks like garbled characters in BI tools.**

Fixed-width **`BINARY(n)`** / **`VARBINARY(n)`** are reported on the wire as `STRING`/`VARCHAR`-class types with **`character_set` = 63 (binary)**. Older Rivet treated every string-class column as Utf8 (`String`) and dumped bytes with UTF‑8 **lossy** encoding — BigQuery `--autodetect` then surfaced **`STRING`** instead of **`BYTES`**. Current Rivet treats charset 63 columns as **`RivetType::Binary`**; re‑export Parquet from this repo and reload.

---

**`bq load`: `utf-8` codec can't decode byte … `.parquet` / "Error decoding JSON schema from file".**

You passed **more than one** local path after the table name (`*.parquet` expanded to multiple files). The CLI interprets an extra positional argument as a **[JSON schema file](https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file)** and UTF‑8-decodes it — Parquet binary then blows up mid‑decode. Fix: **`ls -t … | head -1`** (one Parquet URI) or **`gsutil` + `gs://…/*.parquet`**.

---

**`uid` / `attrs` are NULL in BigQuery (Postgres exports).**

That was a PostgreSQL-only bug: older code tried `String` on `json`/`jsonb` OIDs (`rust-postgres` rejects it), so Utf8 columns were all-null while timestamps/`bytea` worked. Rivet now decodes JSON/JSONB via `postgres_types::Json<serde_json::Value>` and `uuid` via a small adapter. **MySQL:** JSON/`VARCHAR` export path is separate; null `attrs` with MySQL usually means wrong config/binary — run `rivet check` below.

**`amount` / `fee` are NULL.**

- **Postgres:** Utf8 fallback used to call `try_get::<String>` on `numeric` OID (rejected silently → null); decimal path requires `columns:` overrides; numeric Utf8 fallback now uses `PgNumericWire`.
- **MySQL:** `DECIMAL` needs explicit `columns: amount: decimal(18,2)` / `fee: decimal(18,6)` (`dev/workbench/mysql_type_matrix.yaml`) because the Rust client does not surface precision from column metadata alone.

For **BigQuery `NUMERIC`** (not STRING), both YAMLs must keep those `decimal(18,*)` overrides so Arrow writes `Decimal128` into Parquet.

**`amount` / `fee` load as STRING instead of NUMERIC.**

The Parquet file has Utf8 physical columns (`BYTE_ARRAY`). Fix: export with **`columns:`** `decimal(...)` and a Rivet binary that honours them (`cargo install --path . --locked`), then reload.

**Before `bq load`**, confirm mappings (reachable DB):

```bash
cargo build --release
./target/release/rivet check --config dev/workbench/pg_type_matrix.yaml \
  --export type_matrix_pg --type-report --target bigquery
./target/release/rivet check --config dev/workbench/mysql_type_matrix.yaml \
  --export type_matrix_mysql --type-report --target bigquery
```

You want `Decimal128(18, 2)` / `Decimal128(18, 6)` on `amount` / `fee` in both reports.

**Inspect the Parquet file locally** (optional):

```bash
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('YOUR.parquet'))"
```

Decimal columns should appear with a `decimal128(...)` Arrow type — not `string`.

---

See also: [docs/reference/testing.md](../../docs/reference/testing.md#trust-milestone-type-golden-round-trip).
