# CLI behavior matrix — regression guard

Part of the matrix family under [`dev/matrices/`](../matrices/README.md).
Run the full PR tier with `dev/matrices/run.sh --tier=pr`.

Reproduces the 0.7.5 audit: drives the rivet binary through every
subcommand with realistic flag combinations against PostgreSQL and
MySQL fixtures, captures `stdout` / `stderr` / `exit code` per
scenario, and lets a reviewer diff behavior between releases.

## Why

Unit tests pin individual functions; live integration tests in
`tests/live_*.rs` cover specific paths. Neither catches CLI-surface
inconsistencies that show up only when an operator actually types
commands:

- Mojibake in error messages (the `redact_url_passwords`
  double-encoding bug, F-NEW-C in the 0.7.5 audit, was invisible to
  every existing test because none used non-ASCII strings)
- `--force` flag silently mismatching its documented behavior (F1)
- `apply` reading state from the wrong directory when the plan file
  is stored separately from the config (F13)
- `doctor` returning rc=0 even when a probe failed (F-NEW-A)

The matrix surfaces this by running the actual binary, on the actual
docker fixtures, with the actual flag matrix an operator might use.

## Running

```bash
docker compose up -d postgres mysql
cargo build --bin rivet --release
cp target/release/rivet dev/cli_matrix/rivet
cd dev/cli_matrix
./matrix.sh
```

After ~30s the script prints one line per scenario:

```
pg_apply_drift_force          rc=0   stdout=0    stderr=1022   apply --force drifted (F1)
```

Full `stdout`, `stderr`, `cmd`, and `exit_code` of each scenario are
captured under `logs/matrix/<id>/`. The `pa_audit` fixture table is
expected to exist on both engines with 30 rows (the harness creates
it from `cfg/pa_audit.sql`).

## Using as a regression guard

Before tagging a release:

1. Run the matrix against the previous release binary, save into
   `logs/matrix_baseline/`.
2. Run against the new release binary, save into `logs/matrix_new/`.
3. Diff the per-scenario exit codes — any divergence is intentional
   (note in CHANGELOG) or a regression (block release).
4. Spot-check `stderr` size for the negative scenarios: large stderr
   shrinks usually mean a message got dropped; large growth usually
   means a new warning was added (check it's intended).

Two automated guards complement the manual diff:

- `check_rc.sh` — exit-code contract from `expected_rc.txt`.
- `check_msg.sh` — substring contract from `expected_msg.txt`.

`check_rc.sh` catches "command stopped failing where it should fail" or
vice versa. `check_msg.sh` catches the class of regression that rc=0
masks: a key WARN silently dropped, a noisy log printed N times, an
error hint reworded, JSON-errors mode polluted by a stray log line.
Run both in CI; either failing blocks the release.

Scenarios cover doctor, check, run (all flag combos), plan
(format/output/param), apply (stale ± force, drift ± force, corrupt,
missing), state, metrics, schema, journal, validate, reconcile,
repair, init. Each scenario has a short description string so a
reviewer who has not memorised the IDs can still understand the
purpose from the log directory listing.

## Adding scenarios

Edit `matrix.sh` and append a `run` line. The convention:

```bash
run <id> "<one-line description>" -- <command...>
```

`<id>` is `<engine>_<command>_<variant>`. For commands that touch
both engines, add a paired entry under each.
