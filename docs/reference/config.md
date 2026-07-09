# Complete YAML Config Reference

Every field Rivet accepts in a config YAML, grouped by section.

---

## Root

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | object | **yes** | Database connection and global tuning |
| `exports` | list | **yes** | One or more export definitions |
| `notifications` | object | no | Slack / webhook notification settings |

---

## `source`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | `postgres` \| `mysql` \| `mssql` \| `mongo` | **yes** | — | Database type. `mssql` = SQL Server (URL scheme `sqlserver://`); `mongo` = MongoDB (URL scheme `mongodb://`, see [mongodb.md](mongodb.md)). |
| `url` | string | one of url/url_env/url_file or structured | — | Full connection URL (`postgresql://` / `mysql://` / `sqlserver://` / `mongodb://`) |
| `url_env` | string | | — | Env var name containing the URL |
| `url_file` | string | | — | Path to file containing the URL |
| `host` | string | for structured | — | Database hostname |
| `port` | integer | no | `5432` (PG) / `3306` (MySQL) / `1433` (MSSQL) | Database port |
| `user` | string | for structured | — | Database user |
| `password` | string | no | — | **Not recommended** — plaintext; see [Credentials & plan artifacts](#credentials--plan-artifacts) below |
| `password_env` | string | no | — | Env var name containing the password (recommended) |
| `database` | string | for structured | — | Database name |
| `tuning` | object | no | — | Global tuning (see [tuning.md](tuning.md)) |
| `tls` | object | no | — | Transport security (see [TLS](#tls) below). Omit → plaintext + WARN log. |

**Connection approaches** (mutually exclusive):

1. **URL-based**: provide exactly one of `url`, `url_env`, or `url_file`
2. **Structured**: provide `host`, `user`, `database` (+ optional `port`, `password`/`password_env`)

### TLS

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `disable` \| `require` \| `verify-ca` \| `verify-full` | `verify-full` | Enforcement level (mirrors libpq `sslmode` semantics) |
| `ca_file` | string | — | PEM-encoded CA certificate for private trust stores; required for `verify-ca`/`verify-full` against custom CAs |
| `accept_invalid_certs` | boolean | `false` | Dangerous — disables certificate verification. Only honored when explicitly `true`. |
| `accept_invalid_hostnames` | boolean | `false` | Dangerous — disables hostname (SAN/CN) verification. Only honored when explicitly `true`. |

Example (production):

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tls:
    mode: verify-full
    ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
```

Example (local dev only — no TLS):

```yaml
source:
  type: mysql
  host: 127.0.0.1
  port: 3306
  user: dev
  password_env: DEV_PWD
  database: rivet
  tls: { mode: disable }       # explicit opt-out — silences the plaintext WARN
```

Example (SQL Server — `sqlserver://` scheme, port 1433):

```yaml
source:
  type: mssql
  url_env: MSSQL_URL           # sqlserver://user:pass@host:1433/database
  tls:
    ca_file: /etc/ssl/certs/your-sql-server-ca.pem   # private CA, or:
    # accept_invalid_certs: true                      # self-signed dev cert
```

SQL Server always encrypts the login handshake, so TLS is on regardless; the
`tls:` block only controls how the server certificate is trusted. Supported
export modes and types are listed in [compatibility.md](compatibility.md#sql-server-mssql--current-scope).

When `tls:` is omitted entirely, Rivet connects without TLS and emits a WARN so you notice. See [reference/compatibility.md](compatibility.md) for which servers ship TLS-ready and Rivet's dev-environment defaults.

### Credentials & plan artifacts

A `PlanArtifact` (produced by `rivet plan`) is designed to be committed / reviewed; it **must not** carry plaintext credentials. Rivet enforces ADR-0005 **PA9** (`SourceConfig::redact_for_artifact`):

- `password:` field → always stripped from the artifact (set to `None`).
- `url:` containing `scheme://user:pass@…` → userinfo rewritten to `REDACTED`.
- `password_env` / `url_env` / `url_file` → preserved as **references** so apply-time can re-resolve against the apply-environment.

When redaction runs, `rivet plan` logs:

```
WARN plan 'orders': plaintext credentials stripped from artifact —
     apply time must have equivalent env/file-based auth available
```

**Recommendation**: use `password_env` (or `url_env`) everywhere; only use plaintext `password:` for one-off local scripts. See [ADR-0005 PA9](../adr/0005-plan-apply-contracts.md#pa9--artifact-credential-redaction-acr).

---

## `exports[]`

Each entry in the `exports` list defines one export job.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | **yes** | — | Unique identifier for this export |
| `query` | string | one of query/query_file | — | SQL SELECT query |
| `query_file` | string | | — | Path to `.sql` file (relative to config dir) |
| `mode` | `full` \| `incremental` \| `chunked` \| `time_window` \| `cdc` | no | `full` | Export mode. `cdc` = log-based change data capture ([cdc.md](cdc.md)). MongoDB supports `full` + `cdc` only (a document store has no chunked/incremental/time_window). |
| `format` | `parquet` \| `csv` | **yes** | — | Output format |
| `compression` | `zstd` \| `snappy` \| `gzip` \| `lz4` \| `none` | no | `zstd` | Compression codec (low-level; prefer `compression_profile`) |
| `compression_level` | integer | no | codec default | Compression level (low-level; prefer `compression_profile`) |
| `compression_profile` | `none` \| `fast` \| `balanced` \| `compact` | no | — | High-level preset — overrides `compression` and `compression_level`. See [Compression profiles](#compression-profiles) below. |
| `destination` | object | **yes** | — | Where to write output (see below) |
| `verify` | `size` \| `content` | no | `size` | Integrity depth required of `--validate`. `content` checks every part's MD5 against the store's listing (no download) and **fails** validation for any part only size-verified — e.g. a part too large to upload as a single PUT (lower `max_file_size` so it fits) or a backend that exposes no checksum (local FS, streamed multipart). See [Verification depth](#verification-depth) below. |
| `skip_empty` | boolean | no | `false` | Skip file creation if 0 rows |
| `max_file_size` | string | no | — | Split output: `"256MB"`, `"1GB"`, etc. |
| `wave` | integer | no | — | Advisory execution wave (1 = highest priority, runs first). Written by `rivet plan` from the source-aware prioritization score ([ADR-0006](../adr/0006-source-aware-prioritization.md)); consumed by `rivet apply <config>`, which runs exports wave-by-wave in ascending order (no `wave:` runs last). Hand-editable; a later `rivet plan` refreshes it. |
| `parallel_safe` | boolean | no | — | Whether this export is cheap enough (cost class `Low`, < ~100K rows, and not `isolate_on_source`) to run concurrently with its wave-mates under `rivet apply --parallel-export-processes`. Written by `rivet plan`; a heavier export runs alone in its wave (it already chunk-parallelizes internally). Hand-editable. |
| `meta_columns` | object | no | — | Extra columns added to output |
| `quality` | object | no | — | Data quality checks |
| `tuning` | object | no | — | Per-export tuning overrides |
| `source_group` | string | no | — | Logical group for shared source capacity (replica, host). Drives campaign-level warnings in `rivet plan` (advisory only — ADR-0006) |
| `reconcile_required` | boolean | no | `false` | Advisory hint: treat this export as reconcile-sensitive in planning, independent of the `--reconcile` CLI flag (ADR-0006, Epic C) |
| `columns` | map | no | — | Per-column type overrides (see below) |
| `on_schema_drift` | `warn`\|`continue`\|`fail` | no | `warn` | Policy when structural schema drift is detected (see below) |
| `shape_drift_warn_factor` | float | no | `2.0` | Warn when a string/binary column's max byte length grows beyond `N × stored_max`. Set to `0` to disable shape tracking. |
| `parquet` | object | no | — | Parquet row group tuning (Parquet format only). See [Parquet row group tuning](#parquet-row-group-tuning) below. |

### Compression profiles

`compression_profile` is the recommended way to pick a codec. It maps to a `(codec, level)` pair and takes precedence over any `compression` / `compression_level` fields.

| Profile | Codec | Level | Best for |
|---|---|---|---|
| `none` | no compression | — | Debug, local scratch, fast iteration |
| `fast` | snappy | — | Backfills, pilot runs, low-CPU environments |
| `balanced` | zstd | 3 | **Default for production** — good ratio, moderate CPU |
| `compact` | zstd | 9 | Storage- or network-cost-sensitive pipelines |

```yaml
exports:
  - name: events
    format: parquet
    compression_profile: balanced    # zstd level 3
    destination: { type: local, path: ./out }
```

If you need a specific codec that is not covered by the presets, use `compression` + `compression_level` directly and omit `compression_profile`.

---

### Verification depth

`verify` controls how thoroughly `--validate` (and `rivet validate`) checks each
part at the destination:

- **`size`** (default) — confirm each part exists at its recorded `size_bytes`,
  plus manifest self-consistency and `_SUCCESS`. Content is also MD5-checked for
  free whenever the store surfaces a checksum in its listing, but a part without
  one is accepted as size-only.
- **`content`** — require every part's content MD5 to match the store's listing
  checksum (no download). Any part that could only be size-verified **fails**
  validation with an actionable message.

How content verification works: Rivet computes each part's MD5 before upload and
records it in the manifest; the store computes its own (GCS `md5Hash`, S3/Azure
single-PUT ETag) and returns it in object listings. `--validate` compares the two
with **no download**. Parts that upload as a single PUT get a checksum; parts
large enough to stream as multipart / block-list do not — so under
`verify: content`, lower `max_file_size` until parts fit a single PUT (≤ ~64 MiB
by default), or use a backend that exposes a checksum (local FS never does).

The run report and `rivet validate` show coverage explicitly, e.g.
`3 verified (2 md5, 1 size-only)`.

---

### Parquet row group tuning

Parquet row groups affect memory usage during write, compression ratio, and downstream query performance (predicate pushdown, column skipping). When `parquet:` is omitted, Rivet uses the library default of 1,048,576 rows per group, which is optimal for narrow tables but can be large for wide tables.

```yaml
exports:
  - name: events
    format: parquet
    parquet:
      row_group_strategy: auto          # auto | fixed_rows | fixed_memory
      target_row_group_mb: 128          # target Arrow buffer size per group (auto + fixed_memory)
      max_row_group_mb: 256             # optional upper bound (all strategies)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `row_group_strategy` | `auto` \| `fixed_rows` \| `fixed_memory` | `auto` | How to determine row group size |
| `row_group_rows` | integer | — | Exact rows per group; used with `fixed_rows` only |
| `target_row_group_mb` | integer | `128` | Target Arrow buffer per group in MB; used with `auto` and `fixed_memory` |
| `max_row_group_mb` | integer | — | Hard upper bound on group memory in MB (all strategies) |

| Strategy | Behavior |
|---|---|
| `auto` | Estimates row width from schema column types, computes rows-per-group to hit `target_row_group_mb`. Narrow tables get large groups; wide tables get smaller groups. |
| `fixed_rows` | Use `row_group_rows` exactly. Simple and deterministic, but does not adapt to row width. |
| `fixed_memory` | Same math as `auto` (target / estimated row bytes), but the strategy name is explicit in logs. |

**Examples:**

```yaml
# Auto-tune for a wide JSON table — groups sized to ~64 MB
parquet:
  row_group_strategy: auto
  target_row_group_mb: 64
  max_row_group_mb: 128

# Fixed row count — useful when downstream tooling requires exact group sizes
parquet:
  row_group_strategy: fixed_rows
  row_group_rows: 500000
```

> **Note:** `rivet plan` shows the selected strategy and target in the Format section when `parquet:` is configured.
>
> **`rivet init` auto-generates this block** for chunked exports and large full-mode tables, pre-selecting `target_row_group_mb: 64` for wide schemas (≥ 5 text/JSON/bytea columns) and `128` for narrow ones.

---

### `exports[].on_schema_drift` — schema drift policy

Controls what Rivet does when it detects a structural change in the output schema (column added, removed, or retyped) compared to the snapshot stored from the previous run.

| Value | Behavior |
|---|---|
| `warn` | **(default)** Log a warning, store the new schema fingerprint, and continue the run. |
| `continue` | Silently accept — store the new schema, no log output. |
| `fail` | Abort the run with exit code 1. The schema store is **not** updated, so the next run will detect the same change again. |

`fail` is useful in CI pipelines where schema changes must be reviewed before the new shape is exported downstream.

```yaml
exports:
  - name: orders
    on_schema_drift: fail
```

When `fail` triggers, the output file has already been written to the destination (schema check happens post-extraction), but no cursor advance or manifest commit occurs. Re-run after confirming the schema change is intentional, or switch to `warn` to accept it.

---

### `exports[].columns` — per-column type overrides

Override the Arrow type Rivet infers for a specific column. Useful when:
- a `NUMERIC` / `DECIMAL` column has no explicit precision/scale in the source schema (beyond `rivet init`'s default `decimal(38,18)` placeholder), or
- you need a narrower precision for BigQuery NUMERIC compatibility.

```yaml
columns:
  <column_name>: <type>          # applies to every captured table with the column
  "<table>.<column>": <type>     # applies to ONE table, wins over the bare key
```

Supported override types: `decimal(p,s)` / `numeric(p,s)` (both precision and
scale required; precision > 38 produces `Decimal256`), integer widths
(`smallint`/`int`/`bigint`/`int16`/`int32`/`int64`), floats
(`real`/`float4`/`double`/`float8`), `bool`, `text`/`string`/`varchar`,
`uuid`, `json`/`jsonb`, `date`, the `timestamp*` family (naive / `tz` /
`_ns` variants), and binary (`bytea`/`binary`/`varbinary`/`blob`).

Overrides apply to **batch and CDC identically** (the same resolution
surface). Key shapes: a bare column name applies to every captured table that
has the column — on a multi-table CDC export that means ALL of them; a
qualified `"table.column"` key targets one table and **wins** over the bare
key there. A qualified key naming a table the export does not capture — or
used on a query-shaped export — is a config error at load.

Example:

```yaml
exports:
  - name: orders
    query: "SELECT id, amount, fee FROM orders"
    format: parquet
    destination:
      type: local
      path: ./out
    columns:
      amount: decimal(18,2)
      fee: decimal(18,6)
```

**`rivet init` generates these automatically.** When introspecting a table, `rivet init` reads `numeric_precision` and `numeric_scale` from `information_schema.columns`. If both are present, it emits a concrete override (`decimal(p,s)`). If the column is unbounded (`NUMERIC` without explicit precision), `rivet init` emits a **working default** `decimal(38,18)` plus a **`# REVIEW:`** YAML comment — the config header adds a **`# NOTE:`** line, and `rivet init -o …` prints a stderr reminder so you tighten precision when you know the real domain rules:

```yaml
    columns:
      price: decimal(38,18)  # REVIEW: DDL has no numeric(p,s); edit to the real decimal(p,s) …
```

Type overrides are applied at export time and are reflected in `rivet check --type-report` output.

---

Some PostgreSQL types have no Arrow representation and cannot be exported directly. Rivet will report an error listing all unmappable columns before the run starts.

| PostgreSQL type | Reason | Workaround |
|---|---|---|
| `geometry` (PostGIS) | No Arrow equivalent | Cast to text: `ST_AsText(col) AS col` in your query |
| `geography` (PostGIS) | No Arrow equivalent | Cast to text: `ST_AsText(col) AS col` |
| `hstore` | No Arrow equivalent | Cast to JSON text: `hstore_to_json(col)::text AS col` |
| `tsvector`, `tsquery` | No Arrow equivalent | Cast to text: `col::text AS col` |
| `point`, `line`, `polygon`, etc. | No Arrow equivalent | Cast to text: `col::text AS col` |

Use a SQL expression in your `query` field to work around any unsupported type:

```yaml
exports:
  - name: locations
    query: >
      SELECT id, name, ST_AsText(geom) AS geom_wkt
      FROM locations
    format: parquet
    destination:
      type: local
      path: ./out
```

Rivet exports the WKT text as a `Utf8` (string) column. Downstream tools (DuckDB, GeoPandas, QGIS) can reconstruct geometry from WKT.

---

### Mode-specific fields

**Incremental** (`mode: incremental`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `cursor_column` | string | **yes** | — | Primary progression column. Must be **strictly per-row-distinct and monotonically increasing** — resume uses `WHERE cursor > last_value`, so rows that *tie* on the high-watermark value and become visible after it is passed are skipped. A low-resolution `updated_at` (second granularity) can tie; prefer a sequence/identity id or a sub-value-unique timestamp. See [semantics.md → Known non-guarantees](../semantics.md#known-non-guarantees). |
| `cursor_fallback_column` | string | when `coalesce` | — | Fallback column used when primary is `NULL`. Only valid with `incremental_cursor_mode: coalesce` |
| `incremental_cursor_mode` | `single_column` \| `coalesce` | no | `single_column` | `coalesce` progresses on `COALESCE(primary, fallback)`. See [modes/incremental-coalesce.md](../modes/incremental-coalesce.md) and [ADR-0007](../adr/0007-cursor-policy-contracts.md). |

**Chunked** (`mode: chunked`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `chunk_column` | string | yes* | — | Numeric or date/timestamp column to partition by. *Required unless `chunk_by_key` is set (mutually exclusive). |
| `chunk_by_key` | string | yes* | — | Single index-backed UNIQUE NOT NULL column for **keyset (seek)** pagination — the source-safe shape for tables with no single-integer PK (UUID / string / composite). Requires the `table:` shortcut; mutually exclusive with `chunk_column`. See [chunked modes](../modes/chunked.md) and [ADR-0020](../adr/0020-pg-uuid-pk-chunking-asymmetry.md). |
| `chunk_size` | integer | no | `100000` | Rows per chunk (numeric mode), or page size for keyset. Ignored when `chunk_count` is set. |
| `chunk_size_memory_mb` | integer | no | — | Target memory budget per chunk in MB; `chunk_size` is derived from a `pg_class` row-size estimate (`pg_relation_size / reltuples`), clamped to `[10000, 5000000]` rows. **PostgreSQL only**, requires the `table:` shortcut, mutually exclusive with an explicit non-default `chunk_size:`. |
| `chunk_count` | integer | no | — | Divide the column range into exactly this many equal chunks. `chunk_size` is computed dynamically from `min`/`max`. Must be ≥ 1. Mutually exclusive with `chunk_dense` and `chunk_by_days`. |
| `chunk_by_days` | integer | no | — | Enable date chunking: window size in days. Mutually exclusive with `chunk_dense` and `chunk_count`. |
| `parallel` | integer | no | `1` | Concurrent chunk workers |
| `chunk_dense` | boolean | no | `false` | Use `ROW_NUMBER()` for sparse numeric IDs. Mutually exclusive with `chunk_by_days` and `chunk_count`. |
| `chunk_checkpoint` | boolean | no | `false` | Persist per-chunk progress for resume |
| `chunk_max_attempts` | integer | no | — | Max retry attempts per chunk |

**Time-window** (`mode: time_window`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `time_column` | string | **yes** | — | Timestamp column to filter on |
| `time_column_type` | `timestamp` \| `unix` | no | `timestamp` | Column type |
| `days_window` | integer | **yes** | — | Rolling window size in days |

---

## `exports[]` — value-based partitioning

Orthogonal to `mode`: splits rows into Hive-style `col=value/` destination
sub-folders by a date column. See [partitioning.md](../partitioning.md).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `partition_by` | string | no | — | Date/timestamp column to bucket rows by. Requires a `{partition}` token in `destination.path`/`prefix`. NULLs → `col=__HIVE_DEFAULT_PARTITION__/`. Not compatible with `mode: time_window`. |
| `partition_granularity` | `day` \| `month` \| `year` | no | `day` | Bucket width. |

---

## `exports[].meta_columns`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `exported_at` | boolean | `false` | Add `_rivet_exported_at` column (Timestamp UTC; one value per batch) |
| `row_hash` | boolean | `false` | Add `_rivet_row_hash` column — lower 64 bits of `xxHash3-128` over the row, written as `Int64` for fast `PARTITION BY` / `JOIN`. Deterministic across runs; distinguishes NULL from empty string. |

---

## `exports[].quality`

| Field | Type | Description |
|-------|------|-------------|
| `row_count_min` | integer | Fail if fewer rows exported |
| `row_count_max` | integer | Fail if more rows exported |
| `null_ratio_max` | map (column → float) | Fail if null ratio exceeds threshold |
| `unique_columns` | list of strings | Fail if values are not unique |
| `unique_max_entries` | integer | Cap on distinct values tracked per column during uniqueness checks. When reached, a `Warn` is emitted and checking stops for that column. Prevents unbounded memory growth on high-cardinality columns (UUIDs, email addresses, event IDs). |

Uniqueness tracking uses typed xxHash3-64 internally — numeric and binary columns are hashed directly from raw bytes without string formatting. `unique_max_entries` is the primary knob to control memory on very large tables.

Example:

```yaml
quality:
  row_count_min: 100
  null_ratio_max:
    email: 0.05          # email must be <5% null
  unique_columns:
    - id
    - email
  unique_max_entries: 1000000   # stop after 1M unique values; warn if limit hit
```

**Without `unique_max_entries`** — tracking is unbounded. Safe for tables with hundreds of thousands of rows; may use significant RAM on tables with tens or hundreds of millions of distinct values.

**With `unique_max_entries`** — tracking stops at the limit. The export succeeds but the run summary shows a warning. Use when you want a best-effort uniqueness check without memory risk.

---

## `exports[].destination`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `local` \| `s3` \| `gcs` \| `azure` \| `stdout` | **yes** | Destination type |

### Local

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | **yes** | Output directory |

### S3

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bucket` | string | **yes** | S3 bucket name |
| `prefix` | string | no | Key prefix |
| `region` | string | **yes** | AWS region |
| `endpoint` | string | no | Custom S3 endpoint (MinIO, R2) |
| `access_key_env` | string | no | Env var for access key |
| `secret_key_env` | string | no | Env var for secret key |
| `aws_profile` | string | no | AWS credentials profile name |
| `allow_anonymous` | boolean | no | Skip authentication |

### GCS

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bucket` | string | **yes** | GCS bucket name |
| `prefix` | string | no | Object prefix |
| `credentials_file` | string | no | Path to service account JSON (otherwise ADC / `GOOGLE_APPLICATION_CREDENTIALS`) |
| `endpoint` | string | no | Custom GCS endpoint (fake-gcs-server, test doubles) |
| `allow_anonymous` | boolean | no | Skip authentication (public bucket / emulator) |

### Azure

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bucket` | string | **yes** | Blob container name |
| `prefix` | string | no | Blob prefix |
| `account_name` | string | **yes** | Storage account (`<account>.blob.core.windows.net`) — not a secret |
| `account_key_env` | string | one of key/SAS | Env var holding the account key |
| `sas_token_env` | string | one of key/SAS | Env var holding a SAS token (mutually exclusive with `account_key_env`; a leading `?` is trimmed) |
| `allow_anonymous` | boolean | no | Skip authentication (public container / emulator) |

Full auth-flow matrix (account key vs SAS, `endpoint` override, RBAC): [destinations/azure.md](../destinations/azure.md).

### Stdout

No additional fields. Only `type: stdout` is needed.

### Path and prefix placeholders

The `path` (local) and `prefix` (S3 / GCS) fields support template placeholders, substituted at plan-build time:

| Placeholder | Value |
|---|---|
| `{date}` | UTC date as `YYYY-MM-DD` |
| `{export}` | Export name from config |
| `{table}` | Alias for `{export}` |

```yaml
destination:
  type: s3
  bucket: my-data
  prefix: exports/{date}/{export}/
  region: us-east-1
```

With an export named `orders` running on 2026-05-14, this resolves to `exports/2026-05-14/orders/`.

---

## `notifications`

| Field | Type | Description |
|-------|------|-------------|
| `slack` | object | Slack notification config |

### `notifications.slack`

| Field | Type | Description |
|-------|------|-------------|
| `webhook_url` | string | Slack incoming webhook URL |
| `webhook_url_env` | string | Env var containing webhook URL |
| `on` | list | Events to notify on: `failure`, `schema_change`, `degraded` |

Example:

```yaml
notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK
    on: [failure, schema_change]
```

---

## Environment variable interpolation

Any string value can reference environment variables:

```yaml
source:
  url: "postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:5432/mydb"
```

## Query parameters

Queries can use `${key}` placeholders filled by `--param key=value`:

```yaml
exports:
  - name: filtered
    query: "SELECT * FROM orders WHERE region = '${region}'"
```

```bash
rivet run --config export.yaml --param region=us-east
```
