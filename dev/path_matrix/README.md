# Path-topology + data-accounting matrix

Part of the matrix family under [`dev/matrices/`](../matrices/README.md).

Third matrix in the family (after `dev/cli_matrix/` and `dev/cfg_matrix/`).
Where the other two pin **exit codes** and **error messages**, this one
pins two complementary contracts after a successful `rivet run`:

1. **On-disk file layout** (`expected/<id>.layout`) — what files / dirs
   ended up where.
2. **Per-export accounting** (`expected/<id>.summary`) — row counts,
   files produced, status, format/compression — extracted from each
   `summary.json`. Catches "exported 0 rows where 30 were expected"
   regressions that stay at rc=0.

## Why

Exit codes and messages alone do not catch silent layout regressions:

- A refactor that drops `_SUCCESS` is rc=0, no error — but consumers that
  poll for `_SUCCESS` start hanging.
- A change to chunk-file naming (`_chunk0.parquet` → `_part-00000.parquet`)
  is rc=0 — but downstream loaders globbing `*_chunk*.parquet` go blank.
- Moving `.rivet_state.db` from `<config_dir>` to `<CWD>` is rc=0 — but
  the next run finds no cursor and silently re-exports from scratch.
- Adding intermediate directories under `out/` (a "fix" that strips
  `nested/run/` from `path:` so files land in `out/`) is rc=0 — but the
  pipeline overwrites unrelated runs.

None of these would be caught by `cli_matrix` or `cfg_matrix`. This
matrix is a small set of `rivet run` invocations with a frozen,
normalized listing of every file/directory produced.

## Layout

```
cfg/<id>.yaml             — one config per scenario
expected/<id>.layout      — frozen, normalized file listing baseline
logs/<id>/layout          — actual listing from the last run
logs/<id>/layout.diff     — empty if PASS, unified diff if FAIL
normalize.sh              — strips timestamps and run-IDs
matrix.sh                 — runs every scenario in an isolated workdir
gen_fixtures.sh           — regenerates the YAMLs idempotently
```

Each scenario is invoked from its own `logs/<id>/work/` directory (the
config gets copied there), so `.rivet_state.db` and `.rivet/runs/`
stay scenario-local and don't bleed between runs.

## Normalization rules

- `YYYYMMDD_HHMMSS` → `<TS>`     (parquet/csv filename timestamp)
- `YYYYMMDDTHHMMSS.NNN` → `<RUNID>` (`.rivet/runs/<export>_<run>`)
- `_chunkN.parquet` — preserved (chunk numbering IS the contract)
- Output sorted lexicographically

## Scenarios pinned

| id | what it pins |
|---|---|
| p01 | full + parquet + local: `_SUCCESS`, `manifest.json`, single `pa_audit_<TS>.parquet` |
| p02 | full + csv: same shape, `.csv` extension |
| p03 | chunked: N `<TS>_chunk<N>.parquet` files (chunk numbering stable) |
| p04 | multi-export: each export gets its own destination subdir AND its own `.rivet/runs/<export>_<RUNID>/` |
| p05 | stdout destination: zero files under `out/`, but `.rivet/runs/...` still produced |
| p06 | nested relative path: intermediate dirs (`out/p06/nested/run/`) auto-created and preserved |
| p07 | run summary lands at `<config_dir>/.rivet/runs/<RUNID>/{summary.json,summary.md}` (NOT in CWD, NOT in `out/`) and `.rivet_state.db` lands at `<config_dir>` |

## Running

```bash
docker compose up -d postgres
cargo build --bin rivet --release
cp target/release/rivet dev/path_matrix/rivet
cd dev/path_matrix
./matrix.sh
```

Output is one line per scenario:
- `PASS  layout matches expected`
- `FAIL  layout diverged (see logs/<id>/layout.diff)`
- `NEW   no baseline (review logs/<id>/layout, copy to expected/)`

Non-zero exit when at least one scenario FAILs.

## Updating baselines

When a layout change is intentional:

1. Run `./matrix.sh` to capture the new layout in `logs/<id>/layout`.
2. Inspect the diff in `logs/<id>/layout.diff`.
3. Copy `logs/<id>/layout` over `expected/<id>.layout`.
4. Document the change in the CHANGELOG entry that lands the PR.

CI failure on uncommitted divergence is the regression guard. The
normalizer is intentionally narrow: it strips ONLY timestamps and
run-IDs. Anything else that changes shape (new file, renamed file,
deleted directory) appears as a real diff.
