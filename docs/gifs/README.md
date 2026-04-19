# Instructional GIFs

Three short screencasts of Rivet's core workflows, rendered from VHS tape
scripts against the repository's local Docker Compose stack.

Overview screencasts:

| GIF | Scenario | Source |
|-----|----------|--------|
| [basic.gif](basic.gif) | Scaffold config -> doctor -> check -> run -> state (≈25 s) | [basic.tape](basic.tape) |
| [plan-apply.gif](plan-apply.gif) | Plan/Apply: sealed artifact + credential redaction (ADR-0005 PA9) (≈20 s) | [plan-apply.tape](plan-apply.tape) |
| [reconcile-repair.gif](reconcile-repair.gif) | Chunked export + reconcile + targeted repair; committed boundary untouched per ADR-0009 RR4 (≈35 s) | [reconcile-repair.tape](reconcile-repair.tape) |

Short, single-command spots (embedded next to each step of Getting Started):

| GIF | Scenario | Source |
|-----|----------|--------|
| [init-scaffold.gif](init-scaffold.gif) | `rivet init` + `cat orders.yaml` — what scaffolding produces (≈8 s) | [init-scaffold.tape](init-scaffold.tape) |
| [check-verdict.gif](check-verdict.gif) | `rivet check` verdict block: strategy, verdict, suggestion (≈7 s) | [check-verdict.tape](check-verdict.tape) |
| [inspect.gif](inspect.gif) | Post-run inspection: `state show` + `metrics` + `state files` + `state progression` (≈15 s) | [inspect.tape](inspect.tape) |

Mode / planner spots (embedded in `docs/modes/`, `docs/reference/`, `docs/planning/`):

| GIF | Scenario | Source |
|-----|----------|--------|
| [chunked-progress.gif](chunked-progress.gif) | Chunked export on 50 k rows / 10 chunks with `RUST_LOG=info` so per-chunk progress is visible; ends with the structured summary (≈14 s) | [chunked-progress.tape](chunked-progress.tape) |
| [incremental-cursor.gif](incremental-cursor.gif) | Two-run cursor progression — first run exports 10 k rows and saves cursor, second run is `skipped` via `skip_empty: true` (≈14 s) | [incremental-cursor.tape](incremental-cursor.tape) |
| [discover-artifact.gif](discover-artifact.gif) | `rivet init --discover` + `jq` over the JSON artifact — ranked cursor + chunk candidates per table (≈8 s) | [discover-artifact.tape](discover-artifact.tape) |
| [plan-campaign.gif](plan-campaign.gif) | Multi-export `rivet plan`: Priority / Prioritize block per export + Campaign block with `shared_source_heavy_conflict` warning on a shared `source_group` (≈9 s, needs 20 M + 15 M-row fixture) | [plan-campaign.tape](plan-campaign.tape) |

Destination-specific:

| GIF | Scenario | Source |
|-----|----------|--------|
| [doctor-gcs.gif](doctor-gcs.gif) | `rivet doctor` + `rivet run` against real Google Cloud Storage via Application Default Credentials; final `gcloud storage ls` confirms `.rivet_doctor_probe` + Parquet (≈18 s). Requires `gcloud auth application-default login` and write access to `$GCS_DEMO_BUCKET` (default `rivet_data_test`). | [doctor-gcs.tape](doctor-gcs.tape) |

They are linked from the user-facing guides (see "Where they appear" below)
and are intentionally terminal-only: no narration, no cursor movement, no UI
chrome. They show exactly what `rivet` prints.

---

## Regenerating

Prereqs (one-off):

```bash
brew install vhs          # pulls ttyd + ffmpeg as dependencies

docker compose up -d postgres mysql
cargo build --release --bin rivet --bin seed
cargo run --release --bin seed -- --target postgres      # ~500 rows in public.orders
```

Render all three:

```bash
./docs/gifs/render.sh
```

Or just one:

```bash
./docs/gifs/render.sh basic
./docs/gifs/render.sh plan-apply
./docs/gifs/render.sh reconcile-repair
./docs/gifs/render.sh init-scaffold
./docs/gifs/render.sh check-verdict
./docs/gifs/render.sh inspect
./docs/gifs/render.sh chunked-progress
./docs/gifs/render.sh incremental-cursor
./docs/gifs/render.sh discover-artifact
./docs/gifs/render.sh plan-campaign     # creates ~35 M rows in rivet_gif.*
./docs/gifs/render.sh doctor-gcs        # real GCS via ADC; see below
```

The default `render.sh` invocation (no args) renders the ten "always
reproducible" scenarios against Docker Compose (setup per scenario is
ephemeral — tables live under a dedicated `rivet_gif` schema that is
dropped on teardown). `doctor-gcs` is **not** in that list — it needs
`gcloud auth application-default login` and a writable bucket, so it is
opt-in. `plan-campaign` takes ~60 s because it seeds 35 M narrow rows so
the cost-class classifier triggers `shared_source_heavy_conflict`.

`render.sh` creates an ephemeral `/tmp/rivet-gif-<name>` workdir, seeds any
fixture the scenario needs (the `reconcile-repair` tape uses a dedicated
10,000-row `rivet_gif.events` schema that is dropped on exit), invokes
`vhs`, and moves the rendered `.gif` next to the tape.

## Environment the tapes assume

Each tape inherits `DATABASE_URL`, `RIVET_BIN_DIR`, and `PSQL_BIN` from
`render.sh`. The first `Hide` block prepends them to `PATH` and sets a
clean `PS1='rivet-demo $ '` prompt, so the rendered terminal is
deterministic regardless of the user's shell rc.

## Conventions

- **Relative `Output` paths only.** VHS rejects absolute paths.
- **No multi-line `Type` heredocs.** VHS parses every newline as a tape
  command. Fixture YAMLs are written to the workdir by `render.sh`
  *before* `vhs` runs (see `fixture_chunked_setup`).
- **Theme:** Dracula, 14 pt; 1200 x 720 for `basic` / `plan-apply` and
  1280 x 780 for `reconcile-repair` (wider table output).
- **Typing speed:** 30–35 ms/char. Fast enough to keep GIFs short; slow
  enough to read command lines.

## Where they appear

- [README.md](../../README.md) — top-level table of contents.
- [docs/README.md](../README.md) — "Start Here" section.
- [docs/getting-started.md](../getting-started.md):
  - `basic.gif` at "Walkthrough at a glance".
  - `init-scaffold.gif` in Step 3 (`rivet init`).
  - `check-verdict.gif` in Step 5 (`rivet check`).
  - `plan-apply.gif` in Step 6 (optional plan/apply).
  - `inspect.gif` in Step 7 (inspect results).
  - `reconcile-repair.gif` in Step 8 (reconcile / repair).
- [docs/reference/cli.md](../reference/cli.md) — `plan-apply.gif` next to
  `rivet plan`, `reconcile-repair.gif` next to `rivet reconcile`.
- [docs/destinations/gcs.md](../destinations/gcs.md) — `doctor-gcs.gif` in
  the "Verify" section.
- [docs/modes/chunked.md](../modes/chunked.md) — `chunked-progress.gif` in
  "Progress bar (chunked exports)".
- [docs/modes/incremental.md](../modes/incremental.md) —
  `incremental-cursor.gif` in "What happens".
- [docs/reference/init.md](../reference/init.md) —
  `init-scaffold.gif` in "Single table"; `discover-artifact.gif` in
  "Discovery artifact".
- [docs/planning/prioritization.md](../planning/prioritization.md) —
  `plan-campaign.gif` in "Viewing the output".
- [docs/pilot/quickstart-postgres.md](../pilot/quickstart-postgres.md) /
  [docs/pilot/quickstart-mysql.md](../pilot/quickstart-mysql.md) —
  `basic.gif` at the top.
- [docs/pilot/pilot-walkthrough.md](../pilot/pilot-walkthrough.md) —
  `init-scaffold.gif` (Step 1), `plan-apply.gif` (Step 3),
  `inspect.gif` (Step 5), `reconcile-repair.gif` (Steps 6–8).
- [docs/pilot/demo-quickstart.md](../pilot/demo-quickstart.md) —
  `plan-apply.gif` (Step 3), `reconcile-repair.gif` (Step 4).

If you update a tape, re-render, and commit **both** the `.tape` and the
`.gif` together. The tape is the source; the GIF is the build artifact.
