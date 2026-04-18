# Database version support

Rivet is tested against every PostgreSQL and MySQL version listed in the table
below. Each release runs the **full end-to-end suite** — `doctor`, `check`,
every export mode (full / incremental / chunked / time_window), every output
format (CSV / Parquet) with every compression codec, `reconcile`, recovery
scenarios, state management, date-chunking, and `rivet init` — against each
version. No version-specific code paths are skipped; the same Rust driver
builds and the same YAML configs drive every target.

## Supported versions

| Engine     | Versions | Status |
|------------|---------:|--------|
| PostgreSQL |       12 | Supported |
| PostgreSQL |       13 | Supported |
| PostgreSQL |       14 | Supported |
| PostgreSQL |       15 | Supported |
| PostgreSQL |       16 | Supported (primary target) |
| MySQL      |      5.7 | Supported (EOL upstream Oct 2023) |
| MySQL      |      8.0 | Supported (primary target) |

"Primary target" means the version that runs the e2e suite by default in the
local `docker-compose.yaml` top-level `postgres` / `mysql` services. "Legacy"
versions are opt-in under the `legacy` compose profile (see below).

### Why these versions

- **PostgreSQL 12** — oldest mainstream release still in community support
  (final minor release; community support ended Nov 2024, but many managed
  platforms — RDS, Cloud SQL, Azure Database — continue to ship it).
- **PostgreSQL 13–15** — actively supported by upstream.
- **PostgreSQL 16** — current stable at the time of writing; Rivet's default.
- **MySQL 5.7** — EOL upstream in October 2023 but widely deployed in legacy
  systems; keeping it in the matrix prevents silent breakage for those users.
- **MySQL 8.0** — current stable; Rivet's default.

Older releases (PostgreSQL ≤11, MySQL ≤5.6) aren't tested. They may well work —
Rivet's SQL surface is deliberately narrow — but regressions on them aren't
caught by CI.

## Running the compatibility matrix locally

Bring up every server the matrix covers:

```bash
# Primary versions (PG 16, MySQL 8.0) — plus MinIO and fake-gcs for destinations
docker compose up -d postgres mysql minio fake-gcs

# Legacy versions — opt in via the `legacy` profile
docker compose --profile legacy up -d \
    postgres-12 postgres-13 postgres-14 postgres-15 mysql-57

cargo build --release --bin rivet --bin seed
```

Ports assigned (none conflict with the primary services):

| Service       | Port |
|---------------|-----:|
| `postgres`    | 5432 |
| `postgres-12` | 5412 |
| `postgres-13` | 5413 |
| `postgres-14` | 5414 |
| `postgres-15` | 5415 |
| `mysql`       | 3306 |
| `mysql-57`    | 3357 |

Then pick one of:

```bash
# Full e2e suite on every version — seeds each DB, then runs
# dev/e2e/run_e2e.sh against it with URLs retargeted via env.
bash dev/legacy/run_full_matrix.sh

# Just one target
TARGETS="pg-12"     bash dev/legacy/run_full_matrix.sh
TARGETS="mysql-57"  bash dev/legacy/run_full_matrix.sh

# Lighter compat smoke (seed + mode sampler + init) — same config, fewer
# assertions; useful when iterating on compat-sensitive code paths.
bash dev/legacy/run_legacy.sh
```

### How the matrix targets an arbitrary server

`dev/e2e/run_e2e.sh` no longer hardcodes `localhost:5432` / `localhost:3306`.
It reads `RIVET_PG_URL` and `RIVET_MYSQL_URL` from the environment (falling
back to the primary ports if unset), and every e2e YAML uses
`url_env: RIVET_PG_URL` (or `RIVET_MYSQL_URL`). So the same script + configs
drive any target:

```bash
RIVET_PG_URL=postgresql://rivet:rivet@localhost:5412/rivet \
    bash dev/e2e/run_e2e.sh
```

`run_full_matrix.sh` does nothing more exotic than: seed → export those env
vars → invoke `run_e2e.sh`.

## Engine-specific notes

### MySQL 5.7 — window functions

The view `orders_sparse_for_export` used by the chunked-sparse demo queries
uses `ROW_NUMBER() OVER (...)`, which is only available from MySQL 8.0. The
`dev/mysql/init.sql` seeding script creates this view; when 5.7 runs the same
script it fails at container bootstrap with

```
ERROR 1064 (42000) at line 104: You have an error in your SQL syntax …
near '(ORDER BY id) AS chunk_rownum FROM orders_sparse' at line 5
```

Rivet ships a dedicated `dev/mysql/init_57.sql` (identical schema minus the
view) and `docker-compose.yaml` mounts it for the `mysql-57` service. The
`seed` binary detects the server version via `SELECT VERSION()` and
short-circuits the `CREATE OR REPLACE VIEW` when the server reports `5.x`:

```
  note: MySQL 5.7.44 has no window functions — skipping `orders_sparse_for_export` view
```

All other schema (tables, indexes, JSON columns) works unchanged on 5.7.8+.
No production YAML depends on `orders_sparse_for_export`; it's only used by
the chunked-sparse demo.

### MySQL 5.7 — arm64 hosts (Apple Silicon)

There is no official `arm64` image for `mysql:5.7` on Docker Hub. The
`docker-compose.yaml` entry for `mysql-57` pins `platform: linux/amd64` so
Docker Desktop falls back to its amd64 emulator on M-series Macs. Boot is
~2 seconds slower than a native image but otherwise transparent.

### MySQL 5.7 — auth plugin and local clients

MySQL 5.7 defaults to `mysql_native_password`. MySQL 8.0 defaults to
`caching_sha2_password`, and the Homebrew `mysql-client@9` package on macOS
has dropped the plugin library for native_password entirely:

```
ERROR 2059 (HY000): Authentication plugin 'mysql_native_password'
    cannot be loaded: dlopen(...) (no such file)
```

The rust `mysql` crate (version 28, which Rivet depends on) has native_password
built in, so **Rivet itself connects fine**. Only local CLI tools (`mysql`,
`mysqladmin`) may refuse to. When scripts need a reachability probe, they use
a bash `/dev/tcp` test rather than `mysqladmin`:

```bash
if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
    echo "reachable"
fi
```

### PostgreSQL — no TLS by default

Rivet's `dev/e2e/*.yaml` configs connect without TLS (matches the primary
e2e harness). For production, enable transport security explicitly:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tls:
    mode: verify-full           # disable | require | verify-ca | verify-full
    ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
```

See [config.md](config.md#tls) for the full TLS block. This works against
every supported PostgreSQL version (12+).

## What "passes" means per target

Each target in `run_full_matrix.sh` runs 83 assertions against its assigned
server. Status reported by the suite:

```
  pg-12: PASS (83 passed, 0 skipped)
  pg-13: PASS (83 passed, 0 skipped)
  pg-14: PASS (83 passed, 0 skipped)
  pg-15: PASS (83 passed, 0 skipped)
  pg-16: PASS (83 passed, 0 skipped)
  mysql-57: PASS (83 passed, 0 skipped)
  mysql-80: PASS (83 passed, 0 skipped)
```

581 assertions total, and a broken compat path surfaces which target(s)
failed and which specific assertion inside. Per-target logs are written to
`/tmp/rivet_full_matrix/<target>.log`.

## Policy

- **Adding a new supported version**: add a service to
  `docker-compose.yaml` under the `legacy` profile, add the port mapping to
  `run_full_matrix.sh`, run the matrix, land the PR with the new version
  listed in this page's table.
- **Dropping a version**: remove the service from the compose file, remove
  the target from `run_full_matrix.sh`, remove its row from this page,
  and note the change in `CHANGELOG.md`. Dropping a version is a minor-version
  bump (no SemVer guarantees apply to unsupported servers).
