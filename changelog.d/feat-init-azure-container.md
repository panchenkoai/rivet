- **`rivet init` scaffolded GCS and S3 destinations but not Azure.** **Now** `--azure-container <name>
  --azure-account <name>` writes `destination: type: azure` with the same `prefix: exports/<table>/`
  (`cdc/` for a CDC stream), `account_name:` and `account_key_env: AZURE_STORAGE_KEY`, never a key;
  `--azure-endpoint <url>` adds `endpoint:` (Azurite). It excludes `--gcs-bucket`, `--s3-bucket`
  (`RIVET_CLI_OPTION_UNSUPPORTED` / a clap conflict, as between those two) and `--discover`, and it is
  a bucket `--clickhouse-url` can stage in. **What to do:** nothing; set `AZURE_STORAGE_KEY` (or swap in
  `sas_token_env:`) before the first run.
