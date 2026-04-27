# Rivet 0.3.4 — Parallel cards UI for `--parallel-export-processes`

`rivet run --parallel-export-processes` now renders one **card** per export
with a live progress bar, ETA, and rows, and replaces the bar with the
export's final metrics in place when the child finishes. Below the cards a
single aggregate `Run summary` block prints once for the whole run.

![Parallel cards UI](docs/gifs/parallel-cards.gif)

No YAML config changes. Single-process runs and `--parallel-exports`
(in-process, multi-thread) print exactly as before.

## What changed

### Multi-process exports get a real UI

Until 0.3.4 each child wrote its own progress bar to stderr; with four
exports running concurrently the four bars stomped on each other and
produced a mess no one wanted to look at. 0.3.4 fixes that:

- A small NDJSON IPC protocol (`Started`, `ProgressInit`, `Progress`,
  `Finished`) lets children emit structured events to stdout when launched
  with `RIVET_IPC_EVENTS=1`. Children also suppress their own progress bars
  and skip their per-export stderr summary — the parent has all the data
  via `Finished` instead.
- A single dedicated UI thread on the parent multiplexes events from all
  children into one card stack, redraws on every event, and ticks the
  ETA / elapsed-time fields on a 200 ms idle timer when no event arrives.
- Each card is seven lines (header + five fixed-width meta lines + bottom
  line). The bottom line is the live progress bar while the export is
  running and the export's final metrics (`rows`, `files`, `bytes`,
  `duration`, `peak RSS`) once it finishes — same row, in place. Cards
  stay in scrollback after the run.
- If a child's stdout closes without a `Finished` event (crash, OOM,
  `SIGKILL`), the parent marks the card `failed` with a synthetic warning
  line. The run never silently loses an export.

### Hand-rolled ANSI renderer (instead of `indicatif`)

The parent renderer is implemented as raw ANSI escape sequences
(`\x1b[nA`, `\r`, `\x1b[2K`) rather than `indicatif::MultiProgress`. Same
observable output in real terminals, but deterministic across `vhs` /
`ttyd` recordings and CI pipes, where the cursor controls become harmless
no-ops. The rationale and corner cases are documented at the top of
`src/pipeline/parent_ui.rs`.

### `Destination` is `Send + Sync`

Built-in destinations (`local`, `s3`, `gcs`, `bigquery`) were already
thread-safe; the bound is added explicitly so out-of-tree destinations get
a clear compile-time error rather than a runtime data-race.

## Documentation

- `docs/gifs/parallel-cards.gif` (1280 × 780, ~30 s) — recorded against a
  4-export Postgres fixture (`orders`, `users`, `events`, `sessions`,
  ~210 k rows total). Reproducible from a clean Docker Compose stack with
  `./docs/gifs/render.sh parallel-cards`.
- `docs/reference/cli.md` — new `--parallel-export-processes — one card
  per export` subsection embedding `parallel-cards.gif`, the seven-line
  card layout, and a short note on the IPC protocol.

## Security

- **RUSTSEC-2026-0104** — bumped transitive `rustls-webpki`
  0.103.12 → 0.103.13. Reachable panic when parsing a syntactically
  valid empty `BIT STRING` in the `onlySomeReasons` element of a CRL
  `IssuingDistributionPoint` extension, *before* the CRL's signature is
  verified. Rivet does not consume CRLs directly, but the bump closes
  the audit warning end-to-end (`rustls` → `hyper-rustls` /
  `tokio-rustls` / `rustls-platform-verifier` → `reqwest` / `opendal`).
  `cargo audit` is now clean across all 480 dependencies.

## Compatibility

- No YAML config changes. `rivet check` / `rivet plan` behaviour
  unchanged.
- `--parallel-exports` prints exactly as before.
- `--parallel-export-processes` previously had garbled per-export output
  with no aggregate summary; the new behaviour is strictly better but the
  on-screen layout is different. Scripts that scraped the per-child
  `RunSummary` from stderr should switch to `RIVET_IPC_EVENTS=1` +
  `Finished` events on stdout — the format is documented in
  `src/pipeline/ipc.rs` and stable for the 0.3.x line.

## Install

```bash
cargo install rivet-cli --version 0.3.4
```

Full changelog: [`CHANGELOG.md`](CHANGELOG.md#034-2026-04-27).
