# Recipe: MySQL (Cloud SQL) → Google Cloud Storage, end to end

One table from a MySQL instance on Google Cloud SQL, exported to typed Parquet
in a GCS bucket, verified, and ready to be scheduled. About 10 minutes the
first time, then a single `rivet run` per cycle.

Prefer a form? The [quickstart builder](quickstart-builder.md) generates this
exact block for any engine / destination pairing.

## 0 · What you need

| Item | Where it comes from |
|---|---|
| A MySQL user with `SELECT` on the table | `CREATE USER 'rivet_ro'@'%' IDENTIFIED BY '…'; GRANT SELECT ON shop.* TO 'rivet_ro'@'%';` |
| Network path to the instance | either the **Cloud SQL Auth Proxy** on `127.0.0.1:3306`, or the instance's **public IP** with your client IP in *Authorized networks* |
| A GCS bucket that already exists | `gcloud storage buckets create gs://my-exports --location=EU` |
| Write access to that bucket | `roles/storage.objectAdmin` on the bucket for your user or service account |
| `rivet` installed | `brew install panchenkoai/rivet/rivet` (other options: [README § Installation](https://github.com/panchenkoai/rivet/blob/main/README.md#installation)) |

## 1 · Connection string

Pick **one** of the two rows. The difference matters for step 3: a loopback
host needs no TLS flag, a remote host requires one.

```bash
# A. Through the Cloud SQL Auth Proxy (proxy already running on 127.0.0.1:3306)
export DATABASE_URL='mysql://rivet_ro:S3cret%21@127.0.0.1:3306/shop'

# B. Directly to the instance public IP (TLS enforced by Rivet, see step 3)
export DATABASE_URL='mysql://rivet_ro:S3cret%21@34.76.10.20:3306/shop'
```

Special characters in the password must be URL-encoded (`!` → `%21`, `@` →
`%40`, `/` → `%2F`). The URL lives only in this shell variable: `rivet init`
writes `url_env: DATABASE_URL` into the config, never the URL itself.

## 2 · GCS credentials

```bash
# Laptop: Application Default Credentials, once per machine
gcloud auth application-default login

# Server / CI: a service-account key instead
# export GOOGLE_APPLICATION_CREDENTIALS=/secrets/rivet-sa.json
```

On GCE, Cloud Run or GKE with Workload Identity nothing is needed — the
metadata server provides the token.

## 3 · Scaffold the config

```bash
# A. proxy (loopback host)
rivet init --source-env DATABASE_URL --table orders --gcs-bucket my-exports -o rivet.yaml

# B. public IP (remote host → a TLS posture is mandatory)
rivet init --source-env DATABASE_URL --table orders --tls require --gcs-bucket my-exports -o rivet.yaml
```

`--tls require` encrypts the connection without verifying the server
certificate — fine for a first run. For production download the instance's
`server-ca.pem` from the Cloud SQL console and use
`--tls verify-ca --tls-ca ./server-ca.pem` (Cloud SQL certificates are issued
for the instance name, not the IP, so `verify-full` against an IP will fail).

Omit `--table` to scaffold every table in `shop` (one export each); narrow with
`--include 'order*'` / `--exclude 'tmp_*'`.

The generated file looks like this (comments trimmed):

```yaml
source:
  type: mysql
  url_env: DATABASE_URL          # reads the variable you already exported
  tls: { mode: require }         # only with variant B

exports:
  - name: orders
    query: "SELECT `id`, `customer_id`, `total`, `status`, `updated_at` FROM `orders`"
    mode: full                   # init picks full / incremental / chunked from row estimates
    format: parquet
    meta_columns:                # adds _rivet_exported_at / _rivet_row_hash to the output
      exported_at: true
      row_hash: true
    destination:
      type: gcs
      bucket: my-exports
      prefix: exports/orders/
```

## 4 · Preflight

```bash
rivet doctor -c rivet.yaml
rivet check  -c rivet.yaml
```

Expected:

```
[OK]  Source auth (Mysql)
[OK]  Destination GCS(my-exports)
All checks passed.
```

`doctor` writes a tiny `.rivet_doctor_probe` object into the prefix to prove
write access (it stays there and is ignored by manifests). `check` runs
`EXPLAIN` on the query and reports the column types and a cost verdict
(`EFFICIENT` … `UNSAFE`) with a concrete suggestion when something is off.

## 5 · Export

```bash
rivet run -c rivet.yaml --validate --reconcile
```

```
── orders ──
  run_id:      orders_20260905T101500.412
  status:      success
  rows:        184,230
  files:       1
  output:      gs://my-exports/exports/orders/
  validated:   pass
  reconcile:   MATCH (184,230/184,230)
```

`--validate` re-reads every uploaded file and checks its row count;
`--reconcile` runs `SELECT COUNT(*)` on the source query and compares. Both are
optional and cost one extra read each.

## 6 · Look at what landed

```bash
rivet state files -c rivet.yaml
gcloud storage ls gs://my-exports/exports/orders/
```

```
gs://my-exports/exports/orders/orders_20260905_101500_412.parquet
gs://my-exports/exports/orders/manifest.json
gs://my-exports/exports/orders/_SUCCESS
```

`manifest.json` lists every part with its size and MD5; `_SUCCESS` is written
last, so a downstream job that waits for it never reads a half-finished export.

## 7 · Make it incremental and schedule it

If `orders` has a monotonically increasing column (`updated_at`, or an
auto-increment `id`), switch the export so each run fetches only new rows:

```yaml
    mode: incremental
    cursor_column: updated_at
    skip_empty: true             # no file when nothing changed
```

The cursor is stored in `.rivet_state.db` next to the config. Then schedule the
one line that does the work:

```cron
*/15 * * * *  cd /srv/rivet && rivet run -c rivet.yaml --validate >> run.log 2>&1
```

For tables above a few million rows `rivet init` will already have chosen
`mode: chunked` (resumable with `rivet run --resume`) — leave it. For
row-level changes including deletes, scaffold with `--mode cdc` instead; the
source needs `binlog_format=ROW` and `REPLICATION SLAVE, REPLICATION CLIENT`
grants ([reference/cdc.md](../reference/cdc.md)).

## When it does not work

| Symptom | Cause → fix |
|---|---|
| `source: TLS required — refusing to connect to a remote (non-loopback) host` | Variant B without `--tls`. Add `--tls require` (or `verify-ca` + CA file). |
| `Access denied for user` | Wrong password, or the user is restricted to another host pattern. Check with `mysql -h … -u rivet_ro -p`. |
| `Can't connect to MySQL server` / timeout | Client IP not in *Authorized networks*, or the Auth Proxy is not running. |
| `[FAIL] Destination GCS(...)` with `403` | Missing `storage.objects.create` on the bucket — grant `roles/storage.objectAdmin`. |
| `404 Not Found` on the bucket | The bucket does not exist; `doctor` never creates one. |
| `ADC not found` | Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`. |
| `reconcile: MISMATCH` | Rows changed between export and count on a busy table. Re-run, or read from a replica; see [verify-your-export.md](verify-your-export.md). |

Deeper reading: [destinations/gcs.md](../destinations/gcs.md) ·
[cloud-permissions.md](../cloud-permissions.md) ·
[modes/incremental.md](../modes/incremental.md) ·
[pilot/production-checklist.md](../pilot/production-checklist.md).
