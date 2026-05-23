# Azure Blob Storage Destination

Added in 0.7.1.  Third cloud destination after S3 and GCS, on the same
opendal-backed write/read surface and the same M1–M9 manifest trust
contract.  Verified live against a real Azure storage account on
2026-05-21.

## Config block

```yaml
destination:
  type: azure
  bucket: my-container             # Azure container name (Rivet reuses `bucket:` across S3 / GCS / Azure)
  account_name: mystorageacct       # the `<acct>` in `<acct>.blob.core.windows.net`
  account_key_env: RIVET_AZURE_KEY  # env var holding the account key
  prefix: exports/                  # optional object prefix
```

`account_name` is a plain string — it's the public DNS-visible name of
the storage account, **not** a secret (same status as AWS region).  The
account key lives in an env var and is wrapped in `Zeroizing<String>`
inside Rivet so it's wiped from heap on drop.

Rivet auto-derives the endpoint from `account_name` as
`https://<account_name>.blob.core.windows.net`.  Set `endpoint:`
explicitly for Azurite, sovereign clouds (US-Gov, China-Mooncake), or a
custom DNS in front of the storage account.

## Credentials

### Option 1: Storage account name + account key (the 0.7.1 path)

The primary auth flow for 0.7.1.  Account key comes from the Azure
portal (Storage account → Access keys → key1 or key2).  Rotate via the
portal; Rivet has no opinion about rotation cadence.

Shell:

```bash
export RIVET_AZURE_KEY="long-base64-key-from-portal=="
```

Config:

```yaml
destination:
  type: azure
  bucket: my-container
  account_name: mystorageacct
  account_key_env: RIVET_AZURE_KEY
```

To fetch the key from the Azure CLI:

```bash
az storage account keys list \
  --account-name <account> --resource-group <rg> \
  --query "[0].value" -o tsv
```

### Option 2: Azurite emulator (and public read-only containers)

For local development against
[Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite):

```yaml
destination:
  type: azure
  bucket: rivet-e2e
  endpoint: http://127.0.0.1:10000/devstoreaccount1
  allow_anonymous: true
```

`allow_anonymous: true` skips both `account_name` and `account_key_env`.
Rivet refuses to combine `allow_anonymous: true` with explicit
credentials.

Run Azurite via Docker:

```bash
docker run -d --name rivet-azurite -p 10000:10000 \
  mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0
```

### Option 3: SAS token (added in 0.7.2)

A Shared Access Signature token scopes access to a specific container
and time window — useful when you cannot or should not share the
full account key.

Shell:

```bash
export AZURE_STORAGE_SAS_TOKEN="sv=2021-08-06&ss=b&srt=o&sp=rwdlacupitfx&se=2026-06-01T00:00:00Z&st=2026-05-21T00:00:00Z&spr=https&sig=..."
```

Config:

```yaml
destination:
  type: azure
  bucket: my-container
  account_name: mystorageacct
  sas_token_env: AZURE_STORAGE_SAS_TOKEN
```

`account_key_env` and `sas_token_env` are **mutually exclusive** — Rivet
rejects configs that specify both.  The leading `?` is stripped
automatically if you paste the token directly from the Azure portal.

Generate a SAS token via the Azure CLI:

```bash
az storage container generate-sas \
  --account-name <account> --name <container> \
  --permissions rwdl --expiry 2026-06-01 \
  --account-key "$RIVET_AZURE_KEY" -o tsv
```

#### SAS expiry preflight (added in 0.7.4)

Rivet parses the `se=` (signed-expiry) field from the token at construction
time — before any network call is made:

- **Already expired** — Rivet fails fast with:
  ```
  Azure SAS token already expired (se=2026-05-21T00:00:00+00:00). Generate a new SAS and re-export.
  ```
  `rivet doctor` surfaces this as a named category (`sas expired`) with the
  `az storage container generate-sas` hint, so the operator knows exactly
  what to do.

- **Within 60 minutes of expiry** — Rivet logs a `WARN` and continues.
  Useful when a long export was started close to the expiry boundary.

- **No `se=` field** — the token likely uses a stored-access-policy whose
  expiry is server-side.  Rivet accepts it without a warning.

URL-encoded characters in the expiry value (`%3A` for `:`, `%2B` for `+`)
are decoded automatically, so tokens pasted directly from the Azure portal
or from `az storage container generate-sas -o tsv` work without manual
editing.

### Still planned (future releases)

These auth modes are not yet implemented:

- **Service principal** (`tenant_id`, `client_id`, `client_secret_env`) — unattended automation.
- **Managed identity** — Rivet running inside Azure VM / AKS / Functions.
- **Connection string** (`connection_string_env`) — the all-in-one
  `DefaultEndpointsProtocol=https;AccountName=…;AccountKey=…` blob.

## Required RBAC

The account holding the key needs to be able to write to the container.
Predefined roles that work:

- **Storage Blob Data Contributor** — read/write objects (recommended).
- **Storage Blob Data Owner** — adds ACL management on top.

The Azure storage **account key** path bypasses RBAC entirely and grants
full access to every container in the account — that's the trade-off
for simplicity.  Use SAS token (`sas_token_env`) for least-privilege
access scoped to one container and time window.

## Output keys

Files are uploaded as:

```
az://{container}/{prefix}{export_name}_{YYYYMMDD}_{HHMMSS}.{format}
```

Example: `az://my-container/exports/orders_20260521_181423.parquet`.

The `az://` scheme is the HDFS / azcopy convention.  Rivet writes the
same string into the manifest's `destination.uri` field so downstream
consumers can canonicalise object identities across runs.

## Streaming upload

Like S3 and GCS, Rivet streams data directly to Azure Blob without
buffering the entire file in memory.  Peak RSS is proportional to
`batch_size`, not total export size.

## Trust contract

Identical to S3 and GCS:

- Manifest written to `<prefix>manifest.json` (M1).
- `_SUCCESS` marker written last (M2).
- `--validate` and `--reconcile` consult the manifest (M5/M6).
- `--resume` reconciles cumulative committed rows vs the manifest (M8).
- Mid-resume orphans are quarantined via server-side copy + delete
  (M9 — opendal 0.55 returns Unsupported on `rename` for Azure Blob,
  same as S3/GCS).

## Verify

```bash
rivet doctor --config export.yaml
rivet run --config export.yaml
rivet validate --config export.yaml
```

Plain-text expected output:

```
[OK]  Source auth (Postgres)
[OK]  Destination Azure(my-container)

All checks passed.
```

## Troubleshooting

**`AuthenticationFailed: Server failed to authenticate the request`** —
`account_key_env` points to a stale or rotated key, or `account_name`
doesn't match the key.  Refresh from the Azure portal and re-export.

**`ConfigInvalid: endpoint is empty`** — `account_name` not set and no
explicit `endpoint:` either.  Rivet auto-derives the endpoint from
`account_name` when `endpoint:` is unset; supplying neither yields this
error.

**`connection refused` to `127.0.0.1:10000`** — Azurite emulator not
running.  Start with the Docker command above.

**`The specified container does not exist`** — Azure requires the
container to be pre-created (Rivet does NOT auto-create containers, the
same way S3 buckets must exist beforehand):

```bash
az storage container create \
  --account-name <account> --name <container> \
  --account-key "$RIVET_AZURE_KEY"
```

## See also

- [docs/cloud-auth.md](../cloud-auth.md) — full cross-cloud auth-flow
  matrix (S3 / GCS / Azure) with troubleshooting and use-case
  recommendations.
- [docs/destinations/s3.md](s3.md), [docs/destinations/gcs.md](gcs.md) —
  sibling backends, same trust-contract surface.
