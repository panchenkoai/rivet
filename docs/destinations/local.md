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
{export_name}_{YYYYMMDD}_{HHMMSS}.{format}
```

Examples:
- `users_daily_20260406_120000.parquet`
- `orders_incremental_20260406_120000.csv.zst`

For chunked exports, each chunk appends `_chunk{N}`:
- `orders_chunked_20260406_120000_chunk0.parquet`

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

Parts are named: `big_table_20260406_120000_part000.parquet`, `...part001.parquet`, etc.

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
| CSV     | `zstd` | `zstd`, `gzip`, `none` |

CSV with compression produces `.csv.zst` or `.csv.gz`.

## Verify

```bash
rivet doctor --config my_export.yaml
```

Output:

```
[OK] Destination './output' — directory writable
```

## List exported files

```bash
rivet state files --config my_export.yaml
rivet state files --config my_export.yaml --export users_daily --last 5
```
