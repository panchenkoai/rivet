# Release Gate Checklist — v0.5.x

This checklist must pass before tagging a `v0.5.x` release. It implements
roadmap §6.3.

---

## Automated checks (CI)

These run automatically in `nightly.yml` and must all be green:

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test --lib` (2093 tests pass as of 2026-05-15)
- [x] `cargo test --test format_golden`
- [x] `cargo test --test invariants --test journal_invariants --test recovery`
- [x] `cargo test --lib -- pipeline::sink::tests` (stability gate)
- [x] `cargo test --lib -- plan::validate::tests` (compatibility gate)
- [x] `cargo build --release`
- [ ] `cargo audit` (no unpatched CVEs in direct dependencies) — run before tag
- [ ] All live `#[ignore]` integration tests pass (postgres + mysql + S3 + GCS-compat) — run before tag

---

## Benchmark evidence (Phase 2)

Run `./dev/bench/run_bench.sh all` against the bench seed data and attach the
Markdown report to the release notes.

Report: [docs/benchmark_report_v0.5.x.md](benchmark_report_v0.5.x.md) — measured 2026-05-15.

- [x] §4.3 Compression profiles — wall time and output size for `none`/`fast`/`balanced`/`compact`
- [x] §4.4 Row group targets — peak RSS and file size for 32/64/128/256 MB targets on `bench_wide`
- [x] §4.5 Batch memory policies — wall time and RSS for `warn`/`auto_shrink`/no-cap on `bench_wide`
- [x] §4.6 Quality uniqueness — RSS for capped vs uncapped on `bench_hc`

---

## Documentation review

- [x] `docs/best-practices/` — all seven guides complete and examples tested
- [x] `docs/reference/config.md` — `compression_profile`, `parquet`, `quality`, `unique_max_entries` all documented
- [x] `docs/reference/tuning.md` — `max_batch_memory_mb` / `on_batch_memory_exceeded` documented
- [x] `CHANGELOG.md` — v0.5.1 entry written

---

## Backward compatibility

- [x] Old configs without `compression_profile` still work (`compression`/`compression_level` respected)
- [x] Old configs without `parquet:` block still work (library default row group behavior)
- [x] Old configs without `quality:` block still work (no quality checks run)
- [x] Old configs without `tuning.max_batch_memory_mb` still work (no cap applied)

---

## Known limitations (must be noted in release notes)

- Row group `auto` sizing is schema-based and advisory — actual group size may
  differ for variable-width columns (TEXT, JSONB, BYTEA). Not a hard guarantee.
- Uniqueness tracking with `unique_max_entries` is a hash-based quality signal,
  not a cryptographic or warehouse-grade exact distinct count.
- `auto_shrink` adds CPU overhead proportional to the number of splits. For
  extremely wide rows that split 8+ levels, consider lowering `batch_size`
  instead.

---

## Release tag command

```bash
git tag -a v0.5.x -m "v0.5.x: <summary>"
git push origin v0.5.x
```
