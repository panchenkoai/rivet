# Cross-version PG/MySQL smoke matrix

Part of the matrix family under [`dev/matrices/`](../matrices/README.md).

Light-weight regression guard that asserts every supported PG version
(12-16) and MySQL version (5.7, 8.0) agrees on the exit code of a small
core probe set: `doctor`, `check`, `plan` against a full-mode and a
chunked export.

## Why a fourth matrix

`dev/cli_matrix/` and `dev/cfg_matrix/` pin the CLI surface and config
surface against ONE PG + ONE MySQL (the primary 16 / 8.0). They catch
behavior regressions in the binary but cannot catch *version-specific*
regressions like "the chunked-mode introspection probe broke on PG 12
when we switched to a `pg_class.relfilenode` query".

`dev/legacy/run_full_matrix.sh` covers cross-version but runs the
entire e2e suite per version — minutes of runtime, not CI-friendly as
a pre-merge gate.

This matrix sits between: ~14 invocations total (5 PG × 3 probes × 2
modes + 2 MySQL × 3 probes × 2 modes), ~20s wall-clock, gives the
"does every version still work at all" signal.

## Running

```bash
# 1. Bring up legacy DBs (idempotent; skips if already running)
docker compose --profile legacy up -d \
  postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
docker compose up -d postgres mysql

# 2. Seed pa_audit (30 rows) on every version
./seed_all.sh

# 3. Build + copy binary
cargo build --bin rivet --release
cp target/release/rivet dev/cross_version_matrix/rivet

# 4. Probe + check agreement
cd dev/cross_version_matrix
./matrix.sh
./check_cross.sh
```

## Probes

Per-version invocations (templated YAML in `logs/_template*.yaml`):

| probe | command |
|---|---|
| `doctor` | `rivet doctor -c <probe>.yaml` |
| `check` | `rivet check -c <probe>.yaml` |
| `plan_full` | `rivet plan -c <probe>.yaml -e pa_audit --format json` |
| `doctor_chunked` | same as above on the chunked-mode YAML |
| `check_chunked` | -- |
| `plan_full_chunked` | -- |

## What the guard asserts

`check_cross.sh` walks `logs/<version>/<probe>/exit_code` and:

1. For each probe, every PG version that probed must return the SAME rc.
   A divergence (e.g. `pg-12=0, pg-13=1, pg-14-16=0`) is reported with
   per-version detail and exits 1.
2. Same agreement check for MySQL 5.7 vs 8.0.
3. Sanity: at least ONE PG version must succeed at `doctor` — a sweep
   where every PG version failed is catastrophic regression.

## When versions legitimately diverge

Some behavior IS version-dependent — e.g. MySQL 5.7 lacks window
functions, PG 12 lacks `gen_random_uuid()`. When such a divergence
is intentional:

1. Document in the CHANGELOG entry that ships the change.
2. Update the matrix to skip the probe on the affected version, or
   add a per-version expected_rc override (currently not implemented;
   add when the first legitimate divergence appears).

## Limitations

- **rc-only.** We don't check stderr substrings across versions
  (timestamps, addresses, port numbers differ). For message
  contracts use cli_matrix / cfg_matrix.
- **smoke only.** Full feature coverage per version stays in
  `dev/legacy/run_full_matrix.sh`. This matrix exists to catch a
  catastrophic regression early in 20s, not to validate full e2e.
- **arm64 host caveat.** MySQL 5.7 has no arm64 image — runs under
  emulation on Apple Silicon, which can be slow. If the seed takes
  more than ~30s, MySQL 5.7 may have timed out; re-seed manually.
