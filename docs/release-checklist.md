# Release Checklist

The evergreen, version-agnostic checklist that gates every Rivet tag.  Treat
this as **operator discipline**, not as a CI substitute — most items here are
already enforced by automated gates (PR CI, nightly, semantic gates).  The
checklist names them so a release reviewer can see *what was confirmed* and
*how* without grepping the workflows.

For the historical v0.5.x perf/quality gate, see
[best-practices/release-gate.md](best-practices/release-gate.md).

---

## Scope of this document

| In scope | Out of scope |
|---|---|
| What must be green before tagging | One-off perf reports per release |
| What must be smoke-tested manually | Marketing copy / changelog drafting style |
| What must be updated in docs alongside the binary | `cargo publish` mechanics (handled by `release.yml`) |

Tag creation itself is automated by `.github/workflows/release.yml`.  This
checklist is what a maintainer fills out **before** pushing the tag.

---

## 1. Config / schema

- [ ] `cargo test schema_drift` — checked-in `schemas/rivet.schema.json`
      matches the running binary.
- [ ] `rivet schema config | diff - schemas/rivet.schema.json` — no drift.
- [ ] All sample configs in `examples/` parse + validate (offline):
      `cargo test --test examples_parse`
      *(loads every `examples/*.yaml` through `Config::from_yaml`; no DB/network).*
- [ ] `tests/config_parse_errors.rs` — unknown-field + did-you-mean
      regression suite green.

## 2. Local extraction

- [ ] `cargo test --release` — full offline suite (~1300 tests).
- [ ] `cargo test --release -- --ignored` — full live matrix (PG + MySQL,
      MinIO, fake-gcs, Toxiproxy).  Includes the **type golden** parity
      pair on Postgres + MySQL.
- [ ] Postgres + MySQL `e2e` smoke — `dev/e2e/run_e2e.sh` covers the
      83-assertion end-to-end flow.
- [ ] Parquet output round-trips through `arrow-rs` reader (covered by
      `live_parquet_roundtrip` + `live_type_golden`).
- [ ] CSV output round-trips through `csv` reader (covered by
      `format_golden`).
- [ ] `--validate` and `--reconcile` exit zero on a clean run; non-zero
      on a tampered manifest (covered by
      `live_reconcile_repair`, `validate_regression`).
- [ ] Resume after `kill -9` mid-export keeps the prior `_SUCCESS` and
      converges on retry (covered by `live_crash_recovery`,
      `live_chunked_recovery`).

## 3. Cloud smoke (manual)

> Per-PR CI uses MinIO and fake-gcs containers.  Real-cloud verification
> is operator-driven and recorded in
> [docs/cloud-smoke-tests.md](cloud-smoke-tests.md).

- [ ] S3 — `run` + `validate` + `validate --date` + `validate --prefix`.
- [ ] GCS — `run` + `validate` + `validate --date` + `validate --prefix`.
- [ ] Azure (account key) — `run` + `validate`.
- [ ] Azure (SAS token) — `run` + `validate`; SAS-expiry preflight fires
      on a token < 60 min from `se=`.
- [ ] Failed source auth does **not** leak the URL password into stderr,
      `summary.json`, `summary.md`, manifest, or journal.
- [ ] Failed destination auth does **not** leak credentials into the
      same set of artifacts.
- [ ] Update the "Last manually verified" date in
      [docs/cloud-smoke-tests.md](cloud-smoke-tests.md).
- [ ] Update the corresponding row in
      [docs/reliability-matrix.md § Destination coverage](reliability-matrix.md#destination-coverage).

## 4. Security

- [ ] `cargo audit` — no unpatched advisories in declared deps.
- [ ] Secret-redaction tests green: `tests/config_secrets.rs`,
      `tests/validate_secrets.rs` (where present).
- [ ] No new code path emits raw `DATABASE_URL` / `RIVET_STATE_URL` / cloud
      keys into logs, summaries, manifest, journal, or panic backtraces.
      Search: `rg -n 'url|password|secret_key|account_key|sas_token' src/`
      after the diff and audit any new emitter.

## 5. Docs

- [ ] README quickstart matches the CLI surface (`rivet --help`).
- [ ] CHANGELOG updated under the new version heading.
- [ ] Cloud smoke verification date in
      [docs/cloud-smoke-tests.md](cloud-smoke-tests.md) is current.
- [ ] [docs/reliability-matrix.md](reliability-matrix.md) reflects any
      coverage tier changes.
- [ ] [docs/reference/cli.md](reference/cli.md) describes any new flag
      or subcommand.
- [ ] If the version bumps the schema version, `schemas/latest/`
      mirror is regenerated.

## 6. Backward compatibility

For non-major releases:

- [ ] Old configs without the new fields still parse and run.
- [ ] State-DB migration roundtrip green: `tests/state_compat.rs`
      (v1 → vN).
- [ ] CLI flag contract unchanged at the offline level
      (`tests/cli_contract.rs`).

## 7. Release artifacts

- [ ] `cargo build --release` succeeds on Linux x86_64 (PR CI build job).
- [ ] `release.yml` cross-build matrix green (Linux x86_64/arm64,
      macOS arm64/Intel).
- [ ] Docker image (multi-arch via native amd64+arm64 runners) tagged.
- [ ] Homebrew tap PR opened in
      [`panchenkoai/homebrew-rivet`](https://github.com/panchenkoai/homebrew-rivet).
- [ ] crates.io publish dry-run: `cargo publish --dry-run`.

---

## What this checklist does *not* enforce

The point of being explicit:

- **Per-PR real-cloud CI.**  Too costly and noisy at the current project
  stage.  Real S3 / GCS / Azure runs are recorded manually in
  [docs/cloud-smoke-tests.md](cloud-smoke-tests.md).
- **24-hour soak tests.**  Not run.  Tracked in `rivet_roadmap.md`
  § 5.1 as P2 future work.
- **Release-artifact signing / SBOM.**  Tracked in `rivet_roadmap.md`
  § 5.1 as P1/P2 future work.  When shipped, this checklist gains a
  signature-verification step and an SBOM-generation step.
- **Cross-platform binary smoke.**  Per-PR `build-release` only builds
  Linux x86_64.  Release-tag builds run the full matrix; manual
  install verification on macOS and Linux arm64 is operator-driven.

---

## Updating this checklist

This document is intentionally evergreen.  Per-version perf evidence and
exhaustive test counts belong in the changelog or a dedicated report
(see `docs/benchmark_report_v0.5.x.md` for the pattern).  Edit this file
only when:

1. A new gate becomes mandatory (add the row, link the test file).
2. A previous gate is automated end-to-end (move it from the manual
   section into the CI section, or strike it).
3. A new release artifact ships (e.g. a Snap package would add a row
   under § 7).
