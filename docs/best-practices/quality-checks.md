# Quality Checks

Rivet can run lightweight data quality assertions at export time and block the
pipeline if they fail. Quality checks are declared per-export and run as the
data flows through the sink — no separate query is needed.

---

## Available checks

| Check | Field | Severity | Description |
|---|---|---|---|
| Row count minimum | `row_count_min` | Fail | Export fails if fewer rows than threshold |
| Row count maximum | `row_count_max` | Fail | Export fails if more rows than threshold |
| Null ratio | `null_ratio_max` | Fail | Export fails if null fraction exceeds threshold per column |
| Uniqueness | `unique_columns` | Fail | Export fails if duplicate values detected |
| Uniqueness cap | `unique_max_entries` | Warn | Stops tracking after N distinct values; emits a warning |

---

## Row count gates

Useful for detecting empty or truncated source tables:

```yaml
quality:
  row_count_min: 10000   # fail if source returned fewer than 10 000 rows
  row_count_max: 5000000 # fail if source returned more than 5M rows (sanity guard)
```

Both checks fire after all rows are exported, so the partial file is still
written. The export exits non-zero and the manifest records the failure.

---

## Null ratio

Useful for detecting upstream data quality regressions:

```yaml
quality:
  null_ratio_max:
    email: 0.01       # fail if > 1% of email values are null
    user_id: 0.0      # fail if any user_id is null
    description: 0.5  # fail if > 50% of descriptions are null
```

The ratio is computed as `null_count / total_rows` over the full export.
Columns not listed are not checked.

---

## Uniqueness checks

Rivet uses typed xxHash3-64 internally — numeric and binary columns are hashed
from their native bytes without string formatting. This is fast and
memory-efficient for most tables.

```yaml
quality:
  unique_columns: [id, transaction_id]
  unique_max_entries: 1000000
```

### How uniqueness tracking works

For each row in the export, Rivet hashes the value of each `unique_columns`
entry and adds the hash to a per-column `HashSet<u64>`. After all rows are
exported:

```
duplicates = total_rows - distinct_hashes
```

If `duplicates > 0`, the export fails with a message indicating how many
duplicates were found.

### Hash collisions

xxHash3-64 has a collision probability of ~10⁻¹⁸ for random data. For
practical uniqueness checks this is negligible. For cryptographic guarantees
or exact warehouse-grade distinct counting, use a warehouse query directly.

---

## `unique_max_entries` — the most important setting

Without `unique_max_entries`, the uniqueness hash set grows unboundedly with
the number of distinct values. For a 50-million-row UUID column, this means
~400 MB of memory just for the hash set.

**Always set `unique_max_entries` when enabling `unique_columns`.**

```yaml
quality:
  unique_columns: [id, email]
  unique_max_entries: 1000000   # 1M entries ≈ ~8 MB of hash set memory
```

When the cap is reached:
- Tracking stops for that column (subsequent values are not hashed).
- A `Severity::Warn` quality issue is emitted: `"column 'X': uniqueness check
  capped at N entries; result may be incomplete"`.
- The export still succeeds — the warning is advisory, not a hard failure.

If you need exact uniqueness verification on a 50M-row column, set
`unique_max_entries` to at least the expected distinct count, or run a
`SELECT COUNT(DISTINCT ...)` query separately.

### Memory cost of `unique_max_entries`

Each entry in the hash set costs ~8 bytes (a `u64`). HashSet overhead adds
~40–60% for the allocation and load factor.

| `unique_max_entries` | Approximate memory |
|---|---|
| 100 000 | ~1 MB |
| 1 000 000 | ~10 MB |
| 10 000 000 | ~100 MB |
| 50 000 000 | ~500 MB |

For high-cardinality columns (UUIDs, emails, transaction IDs), a cap of
1 000 000–10 000 000 provides a meaningful uniqueness sample without unbounded
memory growth.

---

## Plan validation warning

If `unique_columns` is configured without `unique_max_entries`, Rivet emits a
plan validation warning at export time:

```
[quality-unique-no-cap] export 'orders': unique_columns is configured without
unique_max_entries — uniqueness tracking may grow without bound on large tables.
Add unique_max_entries to cap memory usage.
```

This warning does not block the export. It is visible in `RUST_LOG=warn` output
and in the `rivet plan` summary.

---

## Complete example

```yaml
quality:
  row_count_min: 1000
  row_count_max: 10000000
  null_ratio_max:
    user_id: 0.0
    email: 0.02
  unique_columns: [user_id, email]
  unique_max_entries: 500000
```

---

## Quality checks as signals, not guarantees

Quality checks in Rivet are **fast, in-pipeline quality signals** designed to
catch common data problems (empty tables, unexpected nulls, duplicate primary
keys) without a separate validation query.

They are not a replacement for:
- Warehouse-grade exact distinct counts (`COUNT(DISTINCT ...)`)
- Schema validation (column types, constraints)
- Referential integrity checks (foreign key validation)
- Statistical distribution checks (min/max/median)

For comprehensive data quality, combine Rivet's export-time checks with a
downstream validation tool (dbt tests, Great Expectations, etc.).

---

## See also

- [Config reference — `exports[].quality`](../reference/config.md)
- [Resource-aware extraction](resource-aware-extraction.md)
