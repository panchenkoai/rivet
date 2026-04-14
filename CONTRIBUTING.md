# Contributing

## Toolchain

This project uses Rust **1.94** (edition 2024). The pinned toolchain is defined
in `rust-toolchain.toml` — running `cargo build` will automatically use it if
you have `rustup` installed.

## Before submitting changes

```bash
cargo fmt --all -- --check     # formatting
cargo clippy --all-targets -- -D warnings  # lints (zero warnings policy)
cargo test --all-targets       # 345+ tests, no database needed
```

All three checks must pass. CI enforces them on every push and PR.

## Documentation governance

The canonical user-facing docs live in `docs/`. Before adding or editing documentation:

1. **One question → one canonical location.** Check whether an existing document already owns the answer. Prefer updating it over creating a new file.
2. **Know the role of the document you are editing.**
   - `README.md` — product landing page: description, install, documentation links, milestones. Nothing else.
   - `docs/README.md` — navigation hub for all user docs.
   - `docs/getting-started.md` — first-run onboarding only. Linear, no detours.
   - `docs/modes/*`, `docs/destinations/*`, `docs/pilot/*` — task guides: when to use something, trade-offs, realistic examples.
   - `docs/reference/*` — exact field/flag/command definitions. No onboarding content.
   - `PRODUCT.md`, `rivet_roadmap.md` — internal author docs. Not part of the user onboarding path.
   - `dev/USER_TEST_PLAN.md` — internal UAT checklist. Not user onboarding.
3. **`docs/` is the only canonical user-facing documentation space.** New user content goes in `docs/`, not in root-level markdown files.
4. **Do not add onboarding material to reference docs**, and do not add field-by-field reference detail to task guides.
5. **Every new doc must declare its role** — who it is for, when to read it, and what canonical question it answers.

PR checklist for documentation changes:
- What role does this doc serve?
- Why is an existing doc not sufficient?
- What canonical question does it answer?

## Documentation language

All **user-facing documentation** must be in **English**, including:

- Markdown in the repository (`README.md`, `CHANGELOG.md`, `PRODUCT.md`, this file, and any future docs)
- Comments in `dev/*.sh` and other scripts meant for operators or contributors
- `clap` help strings and `about` text in `src/main.rs`, `src/bin/seed.rs`, and related binaries
- Inline comments in example YAML configs when they explain behavior to humans

Rust API docs (`///` on public items) should also be English when added.

## Tests

- Run `cargo test` before submitting changes.
- For database-dependent behavior, use the Docker Compose services and scripts under `dev/`.
- New code should include unit tests. Integration tests go in `tests/`.
- Golden tests (exact output comparisons) are preferred for format/serialization code.

## Architecture

The codebase is organized into focused modules:

| Module | Purpose |
|--------|---------|
| `config/` | YAML parsing, validation, variable resolution |
| `source/` | Database connectors (Postgres, MySQL) |
| `pipeline/` | Export orchestration, retry, chunking, validation |
| `format/` | Parquet and CSV writers |
| `destination/` | Local, S3, GCS upload |
| `state.rs` | SQLite state store with versioned migrations |
| `preflight/` | Health checks, EXPLAIN analysis, recommendations |
| `tuning.rs` | Tuning profiles (fast/balanced/safe) |
| `quality.rs` | Data quality checks (row count, nulls, uniqueness) |
| `enrich.rs` | Meta columns (_rivet_exported_at, _rivet_row_hash) |
| `notify.rs` | Slack notifications |
| `resource.rs` | RSS memory monitoring |
