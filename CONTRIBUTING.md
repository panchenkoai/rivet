# Contributing

## Documentation language

All **user-facing documentation** must be in **English**, including:

- Markdown in the repository (`README.md`, `PRODUCT.md`, this file, and any future docs)
- Comments in `dev/*.sh` and other scripts meant for operators or contributors
- `clap` help strings and `about` text in `src/main.rs`, `src/bin/seed.rs`, and related binaries
- Inline comments in example YAML configs when they explain behavior to humans

Rust API docs (`///` on public items) should also be English when added.

## Tests

- Run `cargo test` before submitting changes.
- For database-dependent behavior, use the Docker Compose services and scripts under `dev/`.
