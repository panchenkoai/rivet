# Cloud Destinations

Single-page tour of every destination Rivet ships with: what they have in
common, where they differ, and what guarantees you can build downstream
infrastructure on top of.

For the per-backend deep dive, read the dedicated pages:

| Backend | Page |
|---|---|
| Local filesystem | [docs/destinations/local.md](destinations/local.md) |
| Amazon S3        | [docs/destinations/s3.md](destinations/s3.md) |
| Google Cloud Storage | [docs/destinations/gcs.md](destinations/gcs.md) |
| Azure Blob Storage   | [docs/destinations/azure.md](destinations/azure.md) |
| Stdout           | [docs/destinations/stdout.md](destinations/stdout.md) |

For the *credential* matrix (env vars, profile files, identity providers),
see [docs/cloud-auth.md](cloud-auth.md).

---

## Common output contract

Every non-streaming destination (Local, S3, GCS, Azure) produces the same
three artefacts at the resolved prefix on a clean run:

| File | Purpose |
|---|---|
| `part-NNNNNN.<fmt>` | Data parts in commit order. `<fmt>` is `parquet` or `csv`. One per chunk for `chunked` exports; one for `full` / `incremental`. |
| `manifest.json` | ADR-0012 trust contract: every committed part is listed with `size_bytes` and `content_fingerprint`. Schema fingerprint and run identity travel here. |
| `_SUCCESS` | Single line `xxh3:<16-hex>` over the exact bytes of `manifest.json`. Presence implies M5 (every listed part exists at recorded size). |

The contract is *atomic at write boundaries*, not at the prefix:
`manifest.json` is written before `_SUCCESS`, so an Airflow / CI sensor
that polls for `_SUCCESS` never sees a half-built manifest.  A
`HEAD _SUCCESS` is cheap enough that downstream consumers should prefer
it over `GET manifest.json` for the "data ready?" signal.

Schema and resume semantics:

- `schema.json` is reserved for a future release (per-run schema
  snapshot — see the roadmap).  Today the schema fingerprint lives inside
  `manifest.json` under `schema_fingerprint`.
- `_quarantine/<run_id>/` lands on resume when M9 finds an untracked or
  corrupt part.  The original byte location is preserved under that
  subtree for forensics.

---

## Authentication modes

### Local

No credentials.  Permissions come from the OS.  Use `path:` (with
optional `{date}`/`{table}`/`{export}`/`{run_id}` placeholders) to point
at the output directory.

### Amazon S3

| Mode | Fields | Notes |
|---|---|---|
| Static keys | `access_key_env` + `secret_key_env` | Plain IAM access key pair. |
| Static keys + session token | `access_key_env` + `secret_key_env` + `session_token_env` | STS / SSO / IAM Identity Center / AssumeRole / MFA. |
| Profile | `aws_profile` | Reads `~/.aws/credentials` / `~/.aws/config` like the AWS CLI. |
| Default chain | *(none of the above set)* | Env, profile, container, EC2/EKS — same precedence as the AWS SDK. |

`region:` is optional when the SDK can derive one from the profile or env
vars; required otherwise.  `endpoint:` overrides the resolved S3 endpoint
(MinIO, AWS GovCloud, custom domains).

### Google Cloud Storage

| Mode | Fields | Notes |
|---|---|---|
| Service-account file | `credentials_file: /path/to/sa.json` | Long-lived service-account JSON. |
| Service-account env  | *(none set; `GOOGLE_APPLICATION_CREDENTIALS` exported)* | Path to the SA JSON in env. |
| Application Default Credentials | *(none set; `gcloud auth application-default login`)* | Local-dev / workstation auth. |

`bucket:` is required.  `endpoint:` overrides the default `storage.googleapis.com`.

### Azure Blob Storage

<!-- include: shared/azure-auth-modes.md -->

| Mode | Fields | Notes |
|---|---|---|
| Account key | `account_name` + `account_key_env` | Long-lived storage-account key. |
| **SAS token** | `account_name` + `sas_token_env` | Short-lived, scope-limited credential issued out-of-band. |
| Anonymous | `allow_anonymous: true` | Azurite emulator and public read-only containers only. |

`account_key_env` and `sas_token_env` are **mutually exclusive** — picking
both is refused at config-load time with a message that names both
fields.  `account_name` is the prefix in
`<account>.blob.core.windows.net`; an explicit `endpoint:` (Azurite,
sovereign clouds) takes precedence over the derived URL.

The Azure SAS-token body may be pasted with or without the leading `?`
— Rivet trims it transparently so `sv=…&sig=…` and `?sv=…&sig=…` are
both accepted.

<!-- /include: shared/azure-auth-modes.md -->

Future Azure modes (Managed Identity, Service Principal, workload
identity federation) are on the roadmap but not yet supported.

---

## Manifest + `_SUCCESS`

The trust contract is the same on every cloud backend.  See ADR-0012 for
the formal invariants:

- **M1**: parts before manifest.
- **M2**: `_SUCCESS` carries the fingerprint of the exact `manifest.json`
  bytes — fingerprint drift means *something else* wrote that prefix.
- **M5**: with `_SUCCESS` present, every listed part exists at recorded
  size and content fingerprint.
- **M6**: legacy prefixes (no `manifest.json`) are surfaced as
  `legacy_run: true`; `rivet validate` returns success without certifying.
- **M8**: resume against a `_SUCCESS`-marked prefix is refused without
  `--force`; the verifier wants the operator to opt in to re-exporting
  over a completed dataset.
- **M9**: untracked or corrupt parts encountered on resume are moved
  under `_quarantine/<run_id>/` rather than deleted.

---

## Resume behavior

`rivet run --resume` walks the destination prefix, cross-checks against
the state DB and the prior manifest, and decides per-chunk:

- **skip** — chunk already committed.
- **rewrite** — chunk was in-progress (or its part is missing); re-export
  the same key range.
- **quarantine** — untracked or corrupt object at the chunk's part path;
  move to `_quarantine/<run_id>/` and re-export.

Resume preserves both `manifest.json` and `_SUCCESS` only after the run
finishes cleanly.  An interrupted resume leaves the prior `_SUCCESS` in
place so an external sensor polling `_SUCCESS` still sees the most
recent verified dataset.

---

## Validate behavior

`rivet validate` is the standalone counterpart to `rivet run --validate`
(see [docs/destinations/](destinations/) for the per-backend nuance).  It
**never queries the source**: only `HEAD` / `GET _SUCCESS` /
`GET manifest.json` against the resolved destination.

Validate flags:

- `--date YYYY-MM-DD` — resolve `{date}` against the given day instead of
  today.  The flag a "did yesterday's run land cleanly?" Airflow sensor
  needs.
- `--run-id <RID>` — substitute `{run_id}` in the destination template
  (composes with `--date`).
- `--prefix <STRING>` — bypass placeholder resolution and verify exactly
  that prefix.  Refused with multiple exports — see the inline error.

The resolved physical prefix is surfaced in both `--format pretty` and
`--format json` output (`resolved_prefix`) so it's obvious which bytes
were checked.

---

## Reconcile and repair

| Command | Reads source? | Writes source? | Writes destination? |
|---|:---:|:---:|:---:|
| `rivet reconcile` | yes — one `COUNT(*)` per partition | no | no |
| `rivet repair --execute` | yes — re-exports flagged ranges | no | yes — under `_quarantine/` first if a stale object exists |

Both honor the same placeholder resolver as `run` and `validate`.

---

## Quarantine behavior

Path layout under the destination prefix:

```text
<prefix>/
  part-000001.parquet
  part-000002.parquet
  manifest.json
  _SUCCESS
  _quarantine/
    <run_id>/
      part-XXXXXX.parquet   ← evicted object kept verbatim
```

Quarantine is **best-effort**: a successful `copy` followed by a failed
`delete` leaves the object reachable at both paths.  M9 re-trips on the
next resume and the orphan eventually gets moved.

---

## Support matrix

| Destination | Auth | Manifest | `_SUCCESS` | Resume | Validate | Quarantine |
|---|---|:---:|:---:|:---:|:---:|:---:|
| Local | path  | ✅ | ✅ | ✅ | ✅ | ✅ |
| S3    | env / profile / session-token | ✅ | ✅ | ✅ | ✅ | ✅ |
| GCS   | service-account / env / ADC | ✅ | ✅ | ✅ | ✅ | ✅ |
| Azure | account-key / **SAS** / anonymous | ✅ | ✅ | ✅ | ✅ | ✅ |
| Stdout | — | — | — | — | — | — |

---

## Known limitations

- **Object lifecycle policies** — Rivet does not configure
  retention, lifecycle transitions, encryption-at-rest, or replication
  rules on the destination.  Manage those out-of-band (Terraform, console).
- **Azure SAS expiry** — SAS tokens are short-lived by design.  Rivet
  reads the env var once at process start; a long-running export whose
  token expires mid-flight will fail at the next write.  Pair short SAS
  validity with appropriately small exports, or use account-key auth.
- **Eventual consistency on first list** — S3 / GCS / Azure are
  read-after-write consistent for new objects but list operations can
  lag on some backends.  This affects `--resume` only, which uses
  `list_prefix`; the validate path uses targeted `HEAD` requests and is
  consistent.
- **Network egress costs** — Rivet does not enforce a destination /
  source region affinity.  Exporting a 100 GB table from
  `us-east-1` Postgres to an `eu-west-1` bucket goes the long way
  around at full egress price.  Pin source and destination to the same
  region for any non-trivial workload.

---

## Reporting trust-contract issues

A trust-contract violation is treated as a security-grade bug.  See
[SECURITY.md](../SECURITY.md) for the disclosure channel.  Examples:

- `_SUCCESS` present, but a part listed in `manifest.json` is missing.
- `_SUCCESS` fingerprint disagrees with the bytes of `manifest.json`.
- `manifest.json` references the wrong `schema_fingerprint` for the
  parts at the prefix.
- A credential ever appearing in any artefact (`manifest.json`,
  `summary.json`, journal events, log lines).
