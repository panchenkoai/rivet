# Local Filesystem Destination

## Config block

```yaml
destination:
  type: local
  path: ./output                    # directory for output files
```

`path` can be absolute (`/data/exports`) or relative to the working directory.

Rivet creates the directory if it does not exist.

## Output filenames

Files are named automatically:

```
{export_name}_{YYYYMMDD}_{HHMMSS}_{mmm}.{format}
```

The trailing `_mmm` is milliseconds, added so two runs in the same second never overwrite each other.

Examples:
- `users_daily_20260406_120000_123.parquet`
- `orders_incremental_20260406_120000_123.csv` (CSV is always uncompressed — a compression codec on CSV is rejected at config validation, see [Compression](#compression) below)

For chunked exports, each part appends `_chunk{N}` plus a 16-hex random nonce (the nonce guarantees re-runs/repairs never overwrite an existing part):
- `orders_chunked_20260406_120000_chunk0_9f3a1c2b4d5e6f70.parquet`

## File splitting

For large exports, split output into multiple files:

```yaml
exports:
  - name: big_table
    query: "SELECT * FROM big_table"
    mode: full
    format: parquet
    max_file_size: "256MB"          # split when file exceeds this size
    destination:
      type: local
      path: ./output
```

Parts are named: `big_table_20260406_120000_123_part0.parquet`, `..._part1.parquet`, etc. (unpadded part index; the timestamp includes a millisecond field).

Accepted size suffixes: `KB`, `MB`, `GB` (case-insensitive).

## Compression

Compression is applied before writing to disk:

```yaml
exports:
  - name: users
    query: "SELECT * FROM users"
    mode: full
    format: parquet
    compression: zstd               # default for Parquet
    compression_level: 3            # optional: 1 (fast) to 22 (smallest)
    destination:
      type: local
      path: ./output
```

| Format  | Default compression | Options |
|---------|-------------------|---------|
| Parquet | `zstd` | `zstd`, `snappy`, `gzip`, `lz4`, `none` |
| CSV     | `none` | `none` only |

CSV does not support compression — parquet is the compressed format. A
`compression:` other than `none` on a CSV export is rejected at config
validation. Compress CSV output downstream (e.g. `gzip`) if you need it.

## Verify

```bash
rivet doctor --config my_export.yaml
```

Output:

```
[OK]  Destination Local(./output)
```

## List exported files

```bash
rivet state files --config my_export.yaml
rivet state files --config my_export.yaml --export users_daily --last 5
```
