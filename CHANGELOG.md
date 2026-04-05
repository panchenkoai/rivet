# Changelog

## 0.2.0-beta.1 (2026-04-05)

### Architecture

- **Split `pipeline.rs` (2447 lines) into `pipeline/` module** with 7 focused submodules:
  `chunked`, `cli`, `mod` (orchestration), `retry`, `single`, `sink`, `validate`.
- **Split `config.rs` (1708 lines) into `config/` module** with 4 submodules:
  `models`, `resolve`, `tests`, `mod` (validation & loading).
- **Split `preflight.rs` (1425 lines) into `preflight/` module** with 5 submodules:
  `analysis`, `doctor`, `mod` (orchestration), `mysql`, `postgres`.
- All public API paths unchanged — external callers unaffected.

### Reliability

- **Typed error classification**: `classify_error` now checks Postgres `SqlState` codes
  and MySQL numeric error codes before falling back to string matching, giving more
  precise transient-vs-permanent classification for retries.
- **Replaced production `unwrap()` calls** with `expect()` and descriptive messages
  across `pipeline/`, `config/`, `state.rs`, `format/csv.rs`, and `source/`.
- **Versioned SQLite schema migrations**: `schema_version` table tracks applied
  migrations; new databases start at v3, legacy databases are detected and upgraded
  automatically.  Future schema changes only require adding a new migration entry.

### Code quality

- **Zero clippy warnings**: resolved all `collapsible_if`, `too_many_arguments`,
  `derivable_impl`, `manual_clamp`, `if_let_some_result`, `manual_range_contains`,
  `write_literal`, `is_multiple_of`, unused-import, and needless-borrow lints.
- Added `// SAFETY:` documentation on the sole `unsafe` block (`resource.rs` macOS RSS).
- Added crate-level `//!` documentation to `lib.rs`.

### Packaging

- Added `license = "MIT"`, `repository`, `rust-version = "1.85"` to `Cargo.toml`.
- Added `LICENSE` (MIT) file.

### CI

- `.github/workflows/ci.yml` with four jobs: `rustfmt`, `clippy -D warnings`,
  `cargo test`, and `cargo build --release`.

## 0.1.0

Initial release.
