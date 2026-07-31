# Version-matrix dev stand

A separate compose project (`stand`) next to the canonical repo compose: the
same engine set in TWO independent tiers — **batch** (heavy deterministic
common seed, identical data across every SQL engine) and **cdc** (same engine
matrix, very light seed, CDC server config on). Local object stores (MinIO
9000 / fake-gcs 4443 / azurite 10000) are NOT duplicated here — they run in
the canonical `~/rivet` compose project.

## Up / down

```bash
cd dev/stand
./up.sh            # both tiers + post-seed (MSSQL, Mongo-CDC) — idempotent
./up.sh batch      # batch tier only
./up.sh cdc        # cdc tier only
./verify.sh        # cross-engine checksum: seeds must be identical
docker compose --profile batch --profile cdc down   # stop (volumes survive)
```

## Ports (`<base><version>`; creds rivet/rivet, MSSQL sa/Rivet_Passw0rd!)

| Engine        | batch                          | cdc                            |
|---------------|--------------------------------|--------------------------------|
| PostgreSQL 14 | 5514                           | 5614 (wal_level=logical)       |
| PostgreSQL 18 | 5518                           | 5618 (wal_level=logical)       |
| MySQL 5.7     | 3557 (amd64/Rosetta)           | 3657 (ROW binlog)              |
| MySQL 8.0     | 3580                           | 3680 (ROW binlog, FULL meta)   |
| MSSQL 2019    | 1519 (amd64/Rosetta)           | 1619 (Agent on)                |
| MSSQL 2022    | 1522 (amd64/Rosetta)           | 1622 (Agent on)                |
| Mongo 4.4     | 27104                          | 27204 (rs0, `directConnection`)|
| Mongo 5.0     | 27105                          | 27205 (rs0)                    |
| Mongo 8.0     | 27108                          | 27208 (rs0)                    |

## Seeds (`seeds/`)

- `common/{postgres,mysql,mssql}.sql` — users **50k**, orders **200k**,
  events **500k**. Every value is a pure function of the row number `n`
  (modulo arithmetic from fixed epochs), so all six SQL instances hold the
  **same rows**; `verify.sh` asserts identical count/sum tuples (sums in
  integer cents to dodge client decimal formatting).
- `mongo_seed.py` (host python3, stdlib only) — deterministic Extended-JSON
  generator; `up.sh` pipes it into `mongoimport` inside each mongo container.
  Heavy perf collections `orders` 200k + `events` 500k docs on the batch
  instances (same shape/volume class; Mongo is volume-parity, not
  row-parity). `$numberInt`/`$numberLong` wrappers pin identical BSON types
  on every mongo version — no JS seed files, the shells don't get a vote.
- `cdc/*` — same schemas + formulas, 10 rows per table; MySQL variant also
  grants REPLICATION SLAVE/CLIENT; enabling MSSQL CDC (`sp_cdc_enable_db`)
  is left to tests (the Agent is running).

PG / MySQL seed on FIRST boot of an empty volume
(`docker-entrypoint-initdb.d`); MSSQL (no initdb hook) and ALL Mongo
instances (CDC ones have no PRIMARY during initdb) are seeded by `up.sh`,
idempotently.

## Notes

- Data volumes are named per instance (`stand_pg14_batch_data`, …): restarts
  and `down` keep data; `down -v` resets everything (next `up.sh` re-seeds).
- All containers are `restart: unless-stopped` — the stand survives reboots.
- amd64-only images (MySQL 5.7, both MSSQL) run under OrbStack Rosetta.
