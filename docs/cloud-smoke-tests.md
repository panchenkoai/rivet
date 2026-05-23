# Cloud Smoke Tests

This document records the **manual real-cloud verification** performed
before each Rivet release.  It is the operator-discipline counterpart to
the automated PR CI matrix described in
[reliability-matrix.md](reliability-matrix.md).

> Per-PR CI uses MinIO (S3-compatible) and fake-gcs containers.  Real
> S3 / GCS / Azure endpoints are exercised manually here — too expensive
> and too credential-sensitive to run on every push.

---

## Last manually verified

| Backend | Auth mode | Date verified | Verified by |
|---|---|---|---|
| Local FS | path | continuous (CI) | PR CI |
| S3 | env access key | 2026-05-22 | maintainer |
| S3 | session token (STS) | 2026-05-22 | maintainer |
| S3 | AWS profile | 2026-05-22 | maintainer |
| GCS | ADC / service account | 2026-05-22 | maintainer |
| Azure Blob | account key env | 2026-05-21 | maintainer |
| Azure Blob | SAS token env | 2026-05-22 | maintainer |

Update this table as part of the
[release checklist](release-checklist.md#3-cloud-smoke-manual).

---

## Tested scenarios

For each backend, the smoke run covers:

- [ ] Fresh export to an empty prefix — `rivet run` produces parts +
      `manifest.json` + `_SUCCESS`.
- [ ] Manifest fingerprint round-trip — `_SUCCESS` body matches the
      xxh3 of `manifest.json` bytes.
- [ ] `rivet validate` on the just-finished run — exits 0.
- [ ] `rivet validate --date YYYY-MM-DD` against a previous-day prefix
      — exits 0 (historical anchor).
- [ ] `rivet validate --prefix <abs-prefix>` — bypasses placeholder
      resolution, exits 0 against the same physical prefix.
- [ ] `rivet validate --run-id <RID>` — re-checks a specific run.
- [ ] Failed source auth → no URL password in stderr / `summary.json` /
      `summary.md` / `manifest.json` / journal events / log lines.
- [ ] Failed destination auth → no credential in the same artifact set.
- [ ] Cleanup: probe object `.rivet_doctor_probe` removed; no orphaned
      parts under the test prefix.

---

## Per-backend results

### S3 — `2026-05-22`

| Scenario | Result | Notes |
|---|:---:|---|
| Fresh export | ✅ | MinIO + real AWS S3 (us-east-1) |
| `validate` | ✅ | — |
| `validate --date` (historical) | ✅ | Anchor lifts the implicit "today" assumption (v0.7.2) |
| `validate --prefix` | ✅ | — |
| Manifest fingerprint match | ✅ | M2 |
| Auth-failure secret-leak audit | ✅ | URL password redacted; access keys not echoed |

### GCS — `2026-05-22`

| Scenario | Result | Notes |
|---|:---:|---|
| Fresh export | ✅ | fake-gcs + real GCS bucket |
| `validate` | ✅ | — |
| `validate --date` (historical) | ✅ | — |
| `validate --prefix` | ✅ | — |
| Manifest fingerprint match | ✅ | M2 |
| ADC vs explicit `credentials_file` | ✅ | Both paths exercised |
| Auth-failure secret-leak audit | ✅ | Service-account JSON path is logged but contents are not |

### Azure Blob — `2026-05-21` (account key) / `2026-05-22` (SAS)

| Scenario | Result | Notes |
|---|:---:|---|
| Fresh export (account key) | ✅ | `RIVET_AZURE_KEY` env var |
| Fresh export (SAS token) | ✅ | `AZURE_STORAGE_SAS_TOKEN` env var; v0.7.2 path |
| `validate` | ✅ | — |
| `validate --date` (historical) | ✅ | — |
| `validate --prefix` | ✅ | — |
| Endpoint auto-derive from `account_name` | ✅ | Regression caught 2026-05-21; covered by `azure_destination_auto_derives_endpoint_from_account_name` unit test |
| SAS-expiry preflight | ✅ | v0.7.4 — `doctor` warns when `se=` is < 60 min; fails when expired |
| Auth-failure secret-leak audit | ✅ | Account key + SAS token redacted |

---

## What is *not* covered

Manual smoke tests intentionally skip:

- **Long-running SAS-expiry mid-export.**  The preflight catches
  near-expiry tokens before extraction starts; we do not run a
  many-hour export against a deliberately short SAS.
- **Cross-region network instability.**  Toxiproxy chaos coverage exists
  in PR CI (`live_chaos`) but only against MinIO and fake-gcs.
- **Provider outage or throttling.**  Documented as a known limitation in
  [cloud-destinations.md § Known limitations](cloud-destinations.md#known-limitations).
- **Multipart upload interruption** beyond what `live_chunked_recovery`
  exercises against MinIO.
- **Full IAM permission matrix.**  Minimum-required permissions are
  documented in [cloud-permissions.md](cloud-permissions.md); a
  systematic least-privilege matrix is roadmap.
- **Bucket / container lifecycle policies, encryption-at-rest,
  replication.**  Out of scope for Rivet (the operator manages these
  out-of-band).

---

## How to reproduce

The smoke runner expects environment variables matching each backend's
auth mode (see the per-backend pages under
[docs/destinations/](destinations/)).  At minimum:

```bash
# S3 (real AWS bucket, region us-east-1)
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=wJa...
export RIVET_SMOKE_S3_BUCKET=rivet-smoke-${USER}
rivet doctor --config dev/cloud-smoke/s3.yaml
rivet run    --config dev/cloud-smoke/s3.yaml
rivet validate --config dev/cloud-smoke/s3.yaml
rivet validate --config dev/cloud-smoke/s3.yaml --date "$(date -u +%Y-%m-%d)"
rivet validate --config dev/cloud-smoke/s3.yaml --prefix "$(jq -r .resolved_prefix < .rivet/runs/*/summary.json | tail -1)"
```

Equivalent recipes for GCS and Azure live under
[`examples/`](../examples/) (`pg_full_azure_sas.yaml`,
`mysql_full_azure_sas.yaml`, etc.).

---

## Reporting smoke-run failures

If a smoke run regresses on a clean checkout, file an issue tagged
`smoke-regression` and include:

- The exact backend / auth mode that failed.
- The release tag or commit SHA.
- The `rivet doctor` and `rivet run` output (with credentials
  redacted — Rivet's own output should already be clean).
- The resolved prefix from the run summary.

Trust-contract violations (manifest fingerprint drift, missing parts under
`_SUCCESS`, credentials in artifacts) follow the
[security disclosure path](../SECURITY.md#vulnerability-reporting), not
the public issue tracker.
