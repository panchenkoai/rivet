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
| `type` | `postgres` \| `mysql` | **yes** | — | Database type |
| `url` | string | one of url/url_env/url_file or structured | — | Full connection URL |
| `url_env` | string | | — | Env var name containing the URL |
| `url_file` | string | | — | Path to file containing the URL |
| `host` | string | for structured | — | Database hostname |
| `port` | integer | no | `5432` (PG) / `3306` (MySQL) | Database port |
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
| `mode` | `full` \| `incremental` \| `chunked` \| `time_window` | no | `full` | Export mode |
| `format` | `parquet` \| `csv` | **yes** | — | Output format |
| `compression` | `zstd` \| `snappy` \| `gzip` \| `lz4` \| `none` | no | `zstd` | Compression codec |
| `compression_level` | integer | no | codec default | Compression level |
| `destination` | object | **yes** | — | Where to write output (see below) |
| `skip_empty` | boolean | no | `false` | Skip file creation if 0 rows |
| `max_file_size` | string | no | — | Split output: `"256MB"`, `"1GB"`, etc. |
| `meta_columns` | object | no | — | Extra columns added to output |
| `quality` | object | no | — | Data quality checks |
| `tuning` | object | no | — | Per-export tuning overrides |
| `source_group` | string | no | — | Logical group for shared source capacity (replica, host). Drives campaign-level warnings in `rivet plan` (advisory only — ADR-0006) |
| `reconcile_required` | boolean | no | `false` | Advisory hint: treat this export as reconcile-sensitive in planning, independent of the `--reconcile` CLI flag (ADR-0006, Epic C) |

### Mode-specific fields

**Incremental** (`mode: incremental`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `cursor_column` | string | **yes** | — | Primary progression column (should be monotonically increasing) |
| `cursor_fallback_column` | string | when `coalesce` | — | Fallback column used when primary is `NULL`. Only valid with `incremental_cursor_mode: coalesce` |
| `incremental_cursor_mode` | `single_column` \| `coalesce` | no | `single_column` | `coalesce` progresses on `COALESCE(primary, fallback)`. See [modes/incremental-coalesce.md](../modes/incremental-coalesce.md) and [ADR-0007](../adr/0007-cursor-policy-contracts.md). |

**Chunked** (`mode: chunked`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `chunk_column` | string | **yes** | — | Numeric or date/timestamp column to partition by |
| `chunk_size` | integer | no | `100000` | Rows per chunk (numeric mode) |
| `chunk_by_days` | integer | no | — | Enable date chunking: window size in days. Mutually exclusive with `chunk_dense`. |
| `parallel` | integer | no | `1` | Concurrent chunk workers |
| `chunk_dense` | boolean | no | `false` | Use `ROW_NUMBER()` for sparse numeric IDs. Cannot be combined with `chunk_by_days`. |
| `chunk_checkpoint` | boolean | no | `false` | Persist per-chunk progress for resume |
| `chunk_max_attempts` | integer | no | — | Max retry attempts per chunk |

**Time-window** (`mode: time_window`):

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `time_column` | string | **yes** | — | Timestamp column to filter on |
| `time_column_type` | `timestamp` \| `unix` | no | `timestamp` | Column type |
| `days_window` | integer | **yes** | — | Rolling window size in days |

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

Example:

```yaml
quality:
  row_count_min: 100
  null_ratio_max:
    email: 0.05          # email must be <5% null
  unique_columns:
    - id
```

---

## `exports[].destination`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `local` \| `s3` \| `gcs` \| `stdout` | **yes** | Destination type |

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

### Stdout

No additional fields. Only `type: stdout` is needed.

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
