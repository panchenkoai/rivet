# Google Cloud Storage Destination

## Config block

```yaml
destination:
  type: gcs
  bucket: my-gcs-bucket             # GCS bucket name (must already exist)
  prefix: exports/                   # optional object prefix
```

## Credentials

Rivet uses [OpenDAL](https://opendal.apache.org/) for GCS access. Credentials are resolved in this order:

### Option 1: Application Default Credentials (recommended)

If you're running on GCE, Cloud Run, or have `gcloud` configured, no extra config is needed:

```bash
# On a local machine, set up ADC:
gcloud auth application-default login

# Then just use:
rivet run --config export.yaml
```

```yaml
destination:
  type: gcs
  bucket: my-gcs-bucket
```

### Option 2: Service account JSON key

```yaml
destination:
  type: gcs
  bucket: my-gcs-bucket
  credentials_file: /path/to/service-account.json
```

### Option 3: GOOGLE_APPLICATION_CREDENTIALS env var

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
rivet run --config export.yaml
```

## Required IAM permissions

The service account or authenticated user needs:

- `storage.objects.create`
- `storage.objects.delete` (if overwriting)
- `storage.buckets.get` (for `rivet doctor` verification)

The simplest predefined role: **Storage Object Admin** (`roles/storage.objectAdmin`) on the bucket.

## Output keys

Files are uploaded as:

```
gs://{bucket}/{prefix}{export_name}_{YYYYMMDD}_{HHMMSS}.{format}
```

Example: `gs://my-gcs-bucket/exports/orders_20260406_120000.parquet`

## Streaming upload

Rivet streams data directly to GCS without buffering the entire file in memory. This keeps peak RSS proportional to `batch_size`, not total export size.

## Using fake-gcs-server for development

For local development/testing, use [fake-gcs-server](https://github.com/fsouza/fake-gcs-server):

```yaml
# docker-compose.yaml
services:
  fake-gcs:
    image: fsouza/fake-gcs-server
    ports:
      - "4443:4443"
    command: ["-scheme", "http", "-port", "4443"]
```

```yaml
# rivet config
destination:
  type: gcs
  bucket: test-bucket
  endpoint: "http://localhost:4443"
```

Create the bucket first:

```bash
curl -X POST "http://localhost:4443/storage/v1/b?project=test" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-bucket"}'
```

## Verify

```bash
rivet doctor --config export.yaml
```

![rivet doctor + run against real GCS via ADC](../gifs/doctor-gcs.gif)

The GIF above shows the end-to-end flow with **Application Default Credentials** (no `credentials_file:` in the config, no `GOOGLE_APPLICATION_CREDENTIALS` env var — Rivet reads `~/.config/gcloud/application_default_credentials.json` directly):

1. `cat gcs.yaml` — production-shaped YAML (just `type: gcs`, `bucket:`, `prefix:`).
2. `rivet doctor` writes a small `.rivet_doctor_probe` object to verify write access, then reports `[OK] Destination GCS(<bucket>) — All checks passed`.
3. `rivet run --validate` exports 100 rows and uploads the Parquet file.
4. `gcloud storage ls` confirms the probe file **and** the export both landed in the bucket.

Source: [docs/gifs/doctor-gcs.tape](../gifs/doctor-gcs.tape).

Plain-text equivalent output:

```
[OK] Destination 'gs://my-gcs-bucket/exports/' — bucket accessible, write test passed
```

## Troubleshooting

**`403 Forbidden`** -- Check IAM permissions. The service account needs `storage.objects.create`.

**`404 Not Found`** -- The bucket must already exist. Create it: `gsutil mb gs://my-gcs-bucket`.

**ADC not found** -- Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`.
