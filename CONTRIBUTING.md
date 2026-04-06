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
