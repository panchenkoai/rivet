# Zero-code reconciliation runbook (pilot)

Verify a rivet export against the **live** source with nothing but SQL on both
sides. No rivet code involved — the whole point: this check stays valid even
if every rivet-internal guard were wrong.

Requires: a per-row hash column on the source (any deterministic digest of the
business columns). Example (MySQL, generated column — zero app changes):

```sql
ALTER TABLE t ADD COLUMN row_hash CHAR(32)
  AS (MD5(CONCAT_WS('#', id, amount, status))) STORED;
```

The row hash rides through the export like any other column, which removes
the classic cross-engine trap (numeric/text rendering differences under
`CONCAT` — both sides aggregate the *same stored string*).

## Step 1 — cheap aggregates (run daily)

```sql
-- source (MySQL)
SELECT COUNT(*), SUM(amount), MIN(id), MAX(id) FROM t;
```
```sql
-- destination (DuckDB over the exported parquet)
SELECT COUNT(*), SUM(amount), MIN(id), MAX(id)
FROM read_parquet('s3://bucket/prefix/*.parquet');
```

## Step 2 — global row-hash fold (order-independent)

```sql
-- source
SELECT BIT_XOR(CONV(SUBSTRING(row_hash,1,15),16,10)) FROM t;
-- destination
SELECT bit_xor(CAST(concat('0x', substring(row_hash,1,15)) AS UBIGINT))
FROM read_parquet('…/*.parquet');
```

Equal ⇒ every row's full content matches, regardless of order. XOR is blind to
*pairs* of identical compensating differences — step 3 covers localization and
double-checks by range.

## Step 3 — bucket hashes: localize a mismatch to a PK range

```sql
-- both sides, same expression family:
SELECT id DIV 1000, BIT_XOR(CONV(SUBSTRING(row_hash,1,15),16,10))
FROM t GROUP BY 1 ORDER BY 1;        -- MySQL
SELECT id // 1000, bit_xor(CAST(concat('0x', substring(row_hash,1,15)) AS UBIGINT))
FROM read_parquet('…') GROUP BY 1 ORDER BY 1;  -- DuckDB
```

`diff` the two outputs → the diverging bucket names a 1000-row range.

## Step 4 — pinpoint the row inside the bucket

```sql
SELECT id, row_hash FROM t WHERE id DIV 1000 = <bucket> ORDER BY id;
```

`diff` again → the exact id and both hash values.

## The live-source race, and how each step avoids it

- **Closed windows**: filter both sides with `WHERE updated_at <= <yesterday>`
  — an immutable slice has no race. Default daily mode for append-mostly
  tables.
- **CDC converge**: drain to current → measure source → drain again; an empty
  second drain proves no write landed between measure and stream, so the
  comparison is exact at the checkpoint position. Retry on busy tables —
  converges in 1–2 rounds off-peak.
- A *transient* bucket diff on a hot range is churn; a diff that **survives
  two consecutive sync cycles** is real.

## Verified live (2026-07-04, MySQL 8.0 → parquet, 10k rows)

Steps 1–2: byte-equal both sides. One row mutated on the source
(`UPDATE … WHERE id = 4321`): global fold diverged, bucket scan flagged
exactly bucket 4, row scan named id 4321 with both hashes. Detection →
localization → pinpoint, zero code.
