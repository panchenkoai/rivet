# Stdout Destination

## Config block

```yaml
destination:
  type: stdout
```

No additional fields required. Data is written directly to standard output.

## When to use

- Piping data to other tools (`jq`, `duckdb`, `wc -l`)
- Quick previews without creating files
- Integration with other CLI pipelines

## Example: preview as CSV

```yaml
# preview.yaml
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: preview
    query: "SELECT id, name, email FROM users LIMIT 100"
    mode: full
    format: csv
    compression: none               # no compression for stdout readability
    destination:
      type: stdout
```

```bash
rivet run --config preview.yaml | head -20
```

## Example: pipe to DuckDB

```bash
rivet run --config export.yaml | duckdb -c "SELECT count(*) FROM read_csv('/dev/stdin')"
```

## Example: pipe to jq (CSV → JSON lines)

```bash
rivet run --config export.yaml | csvjson | jq '.[] | select(.status == "active")'
```

## Notes

- Only **one export** can use `type: stdout` per config file (multiple exports would intermix output)
- Compression is supported (`zstd`, `gzip`) but makes the output binary — use `compression: none` for human-readable output
- Rivet streams to stdout without buffering the full result
- Progress bars and log messages go to stderr, so they don't interfere with piped data
- `--validate` and `--reconcile` flags work normally — results are printed to stderr

## Verify

```bash
rivet doctor --config preview.yaml
```

Output:

```
[OK] Destination 'stdout' — writable
```
