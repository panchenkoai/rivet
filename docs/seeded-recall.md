# Seeded-defect recall

The harness is measured by whether it catches known product-bug classes when they
come back, on every engine where the code path exists. "The gate found nothing" is
not that measurement; this is.

Each class is a **source patch** under `dev/seeded/`. The runner applies it to a
scratch worktree, builds that tree, runs only the cells declared to catch it, and
records whether they went red. Nothing in the product changes to make this
possible: there is no environment switch or test hook that turns a defect on.

## Running it

```sh
make seeded-recall                                        # every seed, every engine
make seeded-recall SEEDED_ARGS="--seeds committed-every-event --engines postgres"
uv run python -m dev.seeded --self-test                   # manifest + grading logic, no build
```

Inside the release gate it is the `seeded_recall` stage, off by default because it
costs one build per patch:

```sh
make release-oracle-full ARGS=--with-seeded-recall        # or RIVET_ORACLE_SEEDED_RECALL=1
```

It needs the live stand: the same services the declared cells need (the CDC
stand, ClickHouse + fake-gcs, Oracle, the Postgres state server for the
migration seed).

What it does, in order:

1. **Manifest check.** `dev/seeded/seeds.yaml` is parsed. Every seed must name every
   engine in its scope, and every declared cell must exist as a `fn` in the tree.
2. **Scratch tree.** A git worktree of `HEAD` at `target/seeded/tree` with its own
   target dir `target/seeded/target`. Both are kept between runs, so later builds are
   incremental. Only `HEAD` is graded; uncommitted work is not. Override the location
   with `RIVET_SEEDED_DIR`.
3. **STALE check.** `git apply --check` for every patch. If a patch no longer
   applies, the row fails with `seed <name> no longer applies to <file>: update the
   patch`. It is never skipped.
4. **CONTROL.** The union of all declared cells runs once on the unpatched tree. A
   cell that is red, self-skipped or absent here proves nothing, so it is not graded.
5. **One pass per patch.** The runner snapshots the files the patch touches, applies
   it and builds. The build must say `Compiling rivet-cli`, because a `Fresh` build
   would grade the unpatched binary. Then it runs that patch's control-green cells
   and restores the files from the snapshot (never with `git checkout`), and it
   refuses to go on unless the tree is clean again.
6. **Grade and print** a seed × engine table. Logs go to `target/seeded/logs/`.

The cells run under nextest in the scratch tree, with `--retries 0` and with
`RIVET_BIN_OVERRIDE` removed, so they drive the scratch tree's own patched binary.
`RIVET_TEST_STATE_URL` points at a fresh scratch database next to
`RIVET_GATE_STATE_URL`, which is dropped at exit. The shared stand state database
is never touched.

## What a row means

| status | meaning | gate row |
|---|---|---|
| `CAUGHT` | at least one declared cell that was green on the unpatched tree is red under the seed | PASS |
| `MISSED` | every control-green cell stayed green, or the engine declares `cells: []` (the code path exists and nothing covers it) | FAIL |
| `STALE` | the patch no longer applies, or a declared cell names no test | FAIL |
| `BROKEN` | the patched tree does not build, or cargo did not recompile it. A red build is not a catch. | FAIL |
| `NO-CONTROL` | no declared cell is green on the unpatched tree, so the row grades nothing | FAIL |
| `N/A` | the manifest says why the code path does not exist on this engine | none |

A `MISSED` row is a harness finding. Close it with a cell on that engine; do not
delete the row.

## Adding a seed

1. Pick a bug class from history: a fixed defect whose fix commit you can name.
2. Write the smallest source change that brings it back, in a checkout with no other
   edits, then save it as a patch:
   `git diff -- src/the/file.rs > dev/seeded/<seed-name>/<engine>.patch`.
   If the code path is shared by every engine, write one patch,
   `dev/seeded/<seed-name>/shared.patch`, and set it as the seed-level `patch:`.
   Restore the source from a copy you took before the edit.
3. Add the seed to `dev/seeded/seeds.yaml`. Use `scope: source` (postgres, mysql,
   mssql, mongo, oracle) or `scope: state` (postgres-state, sqlite-state), and list
   **every** engine in that scope:
   - `cells: [test_fn, ...]` for the tests expected to catch the seed. Live tests are
     named bare; a unit test is written `lib:<fn>`.
   - `cells: []` where the code path exists but nothing covers it. That row records a
     MISSED, which is the point.
   - `na: <reason>` only where the code path does not exist on that engine.
   - Optionally `lib:` as an extra engine, for an engine-agnostic unit test of a
     shared path. It is reported separately, so it never hides a per-engine gap.
4. Run `uv run python -m dev.seeded --self-test`, then
   `make seeded-recall SEEDED_ARGS="--seeds <seed-name>"`, and check that the row is
   CAUGHT for the reason you expect: read the red cell's log under
   `target/seeded/logs/`.

When the product changes under a patch, the row reports STALE. Re-cut the patch
against the new code. Do not delete the seed.
