# Cloud Permissions

Minimum credentials Rivet needs at each cloud destination — and the
extra capabilities `validate`, `reconcile`, and `--resume` request on
top of write.

For credential **wiring** (env vars, profiles, identity providers) see
[docs/cloud-auth.md](cloud-auth.md).  For the trust contract those
credentials produce (manifest, `_SUCCESS`, quarantine), see
[docs/cloud-destinations.md](cloud-destinations.md).

---

## Why this is split out

Rivet treats writes and reads asymmetrically:

| Operation | What it touches | Why the permission split matters |
|---|---|---|
| `rivet run` | `PUT` parts, `manifest.json`, `_SUCCESS`. May `LIST`/`HEAD` on resume to detect orphan or quarantined parts. | A write-only role is safe for a happy-path job runner that never resumes; add list+head if you ever expect resume. |
| `rivet validate` | `HEAD _SUCCESS`, `GET manifest.json`, `HEAD` each listed part. | Pure read role.  Run from a separate principal in CI / monitoring. |
| `rivet reconcile` | Same as `validate` plus *source* read for `COUNT(*)`. | Destination read remains pure-read.  Source needs the SELECT grants from your normal extraction role. |
| `rivet repair --execute` | Same as `run`, plus `COPY` + `DELETE` if quarantining stale objects (M9). | Adds delete; required for the quarantine path. |

The recommendation: the **first** principal you create gets full
read+write+list+head+delete on the destination prefix; that single role
covers every Rivet command without mode-switching.  Once the workflow
is stable you can split it into a write-bound principal (just `run`)
and a read-bound principal (`validate` from a separate Airflow sensor
or monitoring job).

---

## Amazon S3

### Required for `rivet run`

- `s3:PutObject` on the destination prefix
- `s3:GetBucketLocation` (resolved by SDK at startup)

### Required for `rivet run --resume`, `rivet validate`, `rivet reconcile`

- `s3:GetObject` on the destination prefix
- `s3:ListBucket` on the bucket scoped to the prefix

### Required for `rivet repair --execute` (with quarantine path)

- `s3:DeleteObject` on the destination prefix
- All of the above

### Example IAM policy (least-privilege role for full Rivet surface)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RivetWrite",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::my-data-bucket/exports/*"
    },
    {
      "Sid": "RivetList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::my-data-bucket",
      "Condition": {"StringLike": {"s3:prefix": ["exports/*"]}}
    }
  ]
}
```

Restrict `s3:prefix` if a single bucket holds multiple unrelated
datasets.  Server-side encryption (SSE-KMS) requires
`kms:Encrypt`/`kms:GenerateDataKey` on the configured key — not part of
Rivet's contract; the operator manages it.

---

## Google Cloud Storage

### Required for `rivet run`

- `storage.objects.create` on the destination prefix

### Required for `rivet run --resume`, `rivet validate`, `rivet reconcile`

- `storage.objects.get` on the destination prefix
- `storage.objects.list` on the bucket

### Required for `rivet repair --execute` (with quarantine path)

- `storage.objects.delete` on the destination prefix
- All of the above

### Predefined roles

The closest fit for the full surface is **Storage Object Admin**
(`roles/storage.objectAdmin`).  For pure-write workloads, **Storage
Object Creator** (`roles/storage.objectCreator`) is sufficient but
breaks `--resume` and every read-side command.

For least-privilege, prefer a **custom role** with the four explicit
permissions above scoped to the bucket via IAM conditions on
`resource.name`.

---

## Azure Blob Storage

Azure has two permission models depending on credential mode:

### Account key (`account_key_env`)

Bypasses RBAC.  The key holder has full access to **every container** in
the storage account — read, write, list, delete, set ACLs.  Convenient
but blunt; use SAS for least-privilege.

### SAS token (`sas_token_env`)

Token-scoped permissions.  Generate with the minimum set:

| Permission flag | Why Rivet needs it |
|---|---|
| `r` (read) | `validate`, `reconcile`, `--resume` |
| `w` (write) | `run` |
| `d` (delete) | `repair --execute` quarantine path |
| `l` (list) | `--resume` and `validate` |
| `c` (create) | `run` (some Azure flows require both `c` and `w`) |

`az storage container generate-sas --permissions rwdlc …` covers the
full Rivet surface for one container and one time window.

### Azure RBAC (when using AAD-based credentials — future)

Once Service Principal / Managed Identity support lands (see
[destinations/azure.md § Still planned](destinations/azure.md#still-planned-future-releases)),
the predefined roles will be:

- **Storage Blob Data Contributor** — read/write/delete blobs
  (recommended for the full Rivet surface).
- **Storage Blob Data Reader** — read-only (`validate`, `reconcile`
  from a separate principal).

---

## Local filesystem

Rivet runs as the OS user that invoked it.  The operator is responsible
for:

- Read+write on the destination directory.
- Enough disk space for the largest individual part *and* the staged
  state file (`.rivet_state.db`).  Streaming uploads to cloud avoid
  buffering the full export but still write per-part files locally
  during chunked runs.

---

## Quick reference table

| Capability | S3 action | GCS action | Azure SAS flag |
|---|---|---|---|
| Write parts + manifest + `_SUCCESS` | `s3:PutObject` | `storage.objects.create` | `c`+`w` |
| Read manifest + parts (`validate`, `reconcile`) | `s3:GetObject` | `storage.objects.get` | `r` |
| List for `--resume` and quarantine detection | `s3:ListBucket` | `storage.objects.list` | `l` |
| Delete (quarantine via `repair --execute`) | `s3:DeleteObject` | `storage.objects.delete` | `d` |
| Discover endpoint at startup | `s3:GetBucketLocation` | n/a (handled by SDK) | n/a |

---

## Verifying the role works

`rivet doctor --config rivet.yaml` performs a probe write under the
resolved destination prefix (`.rivet_doctor_probe`) and removes it on
success.  A clean `doctor` run tells you the credential resolves and
has at least write+delete on the prefix.  It does **not** prove
list/head capabilities; the first `rivet run --resume` against an
existing prefix is what surfaces missing list permissions.

For a stricter dry-run, follow the manual cloud smoke flow in
[docs/cloud-smoke-tests.md](cloud-smoke-tests.md).

---

## See also

- [docs/cloud-auth.md](cloud-auth.md) — credential wiring matrix.
- [docs/cloud-destinations.md](cloud-destinations.md) — the trust
  contract every backend implements.
- [docs/destinations/](destinations/) — per-backend deep dives.
- [SECURITY.md](../SECURITY.md) — credential handling and redaction
  guarantees.
