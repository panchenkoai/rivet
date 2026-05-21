# Cloud destination authentication

Rivet talks to S3 / GCS / Azure Blob Storage via [opendal](https://opendal.apache.org/).
Three supported AWS auth flows, three GCS flows, and two Azure flows are
documented below, each with the exact rivet config + shell setup, plus
a "what NOT to use" note for the common confused-by-AWS-CLI-v2 case.

If your auth path isn't listed, the rivet error you'll see most often
is one of:

```text
loading credential to sign http request, source: error sending request
for url (http://169.254.169.254/latest/api/token)
```

That's the **EC2 instance-metadata-service fallback** — opendal didn't
find creds in the configured chain and is now trying IMDS, which is
unreachable on a developer laptop or non-EC2 host.  The fix is always
"give opendal the right credentials before it falls through to IMDS".

---

## AWS S3

### Path A — static IAM access key (long-lived)

The classical case: an IAM user has a long-lived `(access_key_id,
secret_access_key)` pair (looks like `AKIA...`).  No session token, no
rotation worries.  Best for CI, automation, dedicated rivet IAM users.

Shell:

```bash
export RIVET_AWS_ACCESS_KEY=AKIAxxxxxxxxxxxxxxxx
export RIVET_AWS_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Rivet config:

```yaml
destination:
  type: s3
  bucket: my-bucket
  region: eu-north-1
  access_key_env: RIVET_AWS_ACCESS_KEY
  secret_key_env: RIVET_AWS_SECRET_KEY
```

### Path B — temporary credentials with session token (STS / SSO / IAM Identity Center / AssumeRole / MFA / IRSA)

If your access key starts with `ASIA...` rather than `AKIA...`, it's a
**short-lived STS token** and you MUST also pass the session token,
otherwise S3 rejects every request.

This covers a lot of modern AWS setups:

- **AWS IAM Identity Center / AWS Login** (`aws configure` in AWS CLI v2 → "AWS Login"): credentials live in `~/.aws/login/cache/`, not in `~/.aws/credentials`.
- **`aws sts assume-role`** for cross-account access.
- **MFA-protected sessions** (`aws sts get-session-token`).
- **EKS IRSA** (IAM Roles for Service Accounts) / Pod identities.
- **GitHub Actions OIDC** / GitLab JWT-based AWS access.

Shell — bridge from any of the above to env vars rivet understands:

```bash
# AWS CLI v2 helper that prints export commands:
eval "$(aws configure export-credentials --profile default --format env)"

# Now in this shell session:
#   AWS_ACCESS_KEY_ID=ASIAxxxxxxxxxxxxxxxx
#   AWS_SECRET_ACCESS_KEY=...
#   AWS_SESSION_TOKEN=...
#   AWS_CREDENTIAL_EXPIRATION=2026-05-21T16:33:43+00:00
```

Rivet config — point all three env-name fields at the env vars the
helper just exported:

```yaml
destination:
  type: s3
  bucket: my-bucket
  region: eu-north-1
  access_key_env: AWS_ACCESS_KEY_ID
  secret_key_env: AWS_SECRET_ACCESS_KEY
  session_token_env: AWS_SESSION_TOKEN
```

Caveats:

- The token has a **short lifetime** (often 1 hour).  When it expires
  re-run `aws configure export-credentials …` to refresh.
- For long-running pipelines that exceed the token lifetime, prefer
  Path A (static keys) or run a refresh loop in your scheduler.
- Rivet does NOT ship a daemon-mode that re-reads creds during a run —
  the token captured at startup is used throughout.

### Path C — `aws_profile` (only for static-key profiles)

Rivet has a `aws_profile: <name>` config option that uses reqsign's
`AwsDefaultLoader` to read credentials from `~/.aws/config` +
`~/.aws/credentials`.

**This works only when the named profile carries plain static
`aws_access_key_id` + `aws_secret_access_key` lines** (the format AWS
CLI v1 wrote, and AWS CLI v2's "IAM user" mode still writes).

It does **not** work for AWS Login / SSO profiles that store
short-lived sessions in `~/.aws/login/cache/` — reqsign 0.16's loader
doesn't read that format and falls through to IMDS, hanging or failing
with the error quoted above.

If you have an AWS Login profile, use **Path B** instead.

```yaml
destination:
  type: s3
  bucket: my-bucket
  region: eu-north-1
  aws_profile: rivet-prod
```

### What NOT to use

- **Mixing `aws_profile` with `access_key_env`/`session_token_env`**:
  the explicit env-var fields take precedence at the opendal level,
  but this leaves the reqsign default-chain still wired up and can
  trigger surprise IMDS lookups.  Pick one path.
- **`AWS_PROFILE` env var alone**: rivet doesn't read it.  Either
  set `aws_profile:` in the config or use the env-var path.

---

## Google Cloud Storage

### Path A — Application Default Credentials (developer laptop)

If you ran `gcloud auth application-default login`, ADC writes a token
to `~/.config/gcloud/application_default_credentials.json`.  Rivet
auto-detects this and uses it transparently:

```yaml
destination:
  type: gcs
  bucket: my-bucket
  prefix: exports/
```

No `credentials_file:` needed.  See `gcs_auth::try_authorized_user_token`
in `src/destination/gcs_auth.rs` for the detection.

### Path B — Service account JSON

For CI / production, point at a service-account key file:

```yaml
destination:
  type: gcs
  bucket: my-bucket
  prefix: exports/
  credentials_file: /etc/rivet/sa.json
```

Or via env (opendal honours `GOOGLE_APPLICATION_CREDENTIALS`):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/etc/rivet/sa.json
```

```yaml
destination:
  type: gcs
  bucket: my-bucket
  prefix: exports/
```

### Path C — Anonymous / emulator

For `fake-gcs-server` / GCS emulator setups:

```yaml
destination:
  type: gcs
  bucket: rivet-e2e
  endpoint: http://localhost:4443
  allow_anonymous: true
```

Rivet disables both VM metadata probing and the standard config-load
chain when `allow_anonymous: true` so the emulator path works on a
host that has unrelated GCS profiles configured.

---

## Azure Blob Storage

`type: azure` uses Azure's "container" terminology — the existing `bucket:`
field carries the container name (rivet keeps a single field for the
"top-level namespace inside the cloud account" across S3 / GCS / Azure).

### Path A — Storage account name + account key

The primary auth flow for the 0.7.1 MVP.  Account key is a long-lived
secret string from the Azure portal (Storage account → Access keys → key1
or key2).  Rotate it via the portal; rivet wipes the in-memory copy on
drop via `Zeroizing`.

Shell:

```bash
export RIVET_AZURE_KEY="long-base64-key-from-portal=="
```

Rivet config:

```yaml
destination:
  type: azure
  bucket: my-container            # Azure container name
  account_name: mystorageacct      # the `<acct>` in `<acct>.blob.core.windows.net`
  account_key_env: RIVET_AZURE_KEY
```

`account_name` is a plain string in YAML — it's not a secret, it's the
public DNS-visible name of the storage account (same status as AWS region
or GCS bucket name).

Rivet auto-derives the endpoint from `account_name` as
`https://<account_name>.blob.core.windows.net` — operators only need to
set `endpoint:` for Azurite, sovereign clouds (US-Gov, China-Mooncake),
or a custom DNS in front of the storage account.

### Path B — Azurite emulator / public-read containers

For local development against [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite):

```yaml
destination:
  type: azure
  bucket: rivet-e2e
  endpoint: http://127.0.0.1:10000/devstoreaccount1
  allow_anonymous: true
```

`allow_anonymous: true` skips both `account_name` and `account_key_env`.
Use it only for emulators or genuinely public read-only containers; rivet
will refuse to combine `allow_anonymous: true` with explicit credentials.

### What NOT to use yet (planned for 0.7.2)

The 0.7.1 release only ships the static account-key path.  These flows
will land in 0.7.2 as additive config fields (no breaking changes):

- **SAS token** (`sas_token_env`) — pre-signed URLs, time-limited scope.
- **Service principal** (`tenant_id`, `client_id`, `client_secret_env`) — for unattended automation.
- **Managed identity** — for rivet running inside Azure VMs / AKS / Functions.
- **Connection string** (`connection_string_env`) — the all-in-one
  `DefaultEndpointsProtocol=https;AccountName=…;AccountKey=…` blob.

Until those land, the bridge from any of the above to rivet is: extract
the `AccountKey` value into an env var and use Path A.

---

## S3-compatible storage (MinIO, R2, etc.)

Same as AWS Path A above + an explicit `endpoint:` URL.  Static keys
only — STS / temporary credentials are an AWS-specific concept.

```yaml
destination:
  type: s3
  bucket: rivet-test
  endpoint: http://localhost:9000
  region: us-east-1
  access_key_env: MINIO_ACCESS_KEY
  secret_key_env: MINIO_SECRET_KEY
```

Cloudflare R2, Wasabi, Backblaze B2 etc. follow the same shape — the
S3-compatible API gives them all the same authentication path.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `loading credential to sign http request, source: error sending request for url (http://169.254.169.254/...)` then timeout | IMDS fallback — credentials never resolved.  See above sections. |
| `InvalidAccessKeyId` / `SignatureDoesNotMatch` | Static key + session-token mismatch.  If your `access_key_id` starts with `ASIA…`, you MUST pass `session_token_env` too. |
| `403 Forbidden` on PutObject | Region mismatch (key for one region used against another) or insufficient IAM permission (need `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` for the bucket / prefix). |
| `connection refused` to `localhost:9000` | MinIO not running.  `docker compose up -d minio` from the repo root. |
| GCS auth works in `gcloud` but rivet hangs | Likely ADC has expired.  Re-run `gcloud auth application-default login`. |
| Azure: `AuthenticationFailed: Server failed to authenticate the request` | `account_key_env` points to a stale/rotated key, or `account_name` doesn't match the key.  Refresh from the Azure portal. |
| Azure: `connection refused` to `127.0.0.1:10000` | Azurite emulator not running.  `azurite --location /tmp/azurite &` or `docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite`. |

---

## Recommended setups by use case

- **Local dev → MinIO**: Path A static keys, `endpoint: http://localhost:9000`.
- **Local dev → real AWS S3**: Path B (export creds via `aws configure export-credentials …`).
- **CI / GitHub Actions → real AWS S3**: Path B with OIDC-issued temporary creds (set the env vars from the GitHub `aws-actions/configure-aws-credentials` step output).
- **Production / Airflow / Dagster → S3**: Path A with a dedicated IAM user, key rotation handled by your secret store.
- **Local dev → real GCS**: Path A with `gcloud auth application-default login`.
- **Production → GCS**: Path B with a service account JSON.
- **Local dev → Azurite**: Azure Path B (`allow_anonymous: true`, `endpoint: http://127.0.0.1:10000/devstoreaccount1`).
- **Production → Azure Blob Storage**: Azure Path A with `account_key_env` sourced from your secret store (Key Vault, doppler, sops, etc.).  SAS / Service Principal flows land in 0.7.2.
