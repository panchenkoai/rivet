# Release Gate Checklist — v0.5.x

This checklist must pass before tagging a `v0.5.x` release. It implements
roadmap §6.3.

---

## Automated checks (CI)

These run automatically in `nightly.yml` and must all be green:

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test --lib` (922+ unit tests)
- [ ] `cargo test --test format_golden` (31+ golden tests)
- [ ] `cargo test --test invariants --test journal_invariants --test recovery`
- [ ] `cargo test --lib -- pipeline::sink::tests` (stability gate)
- [ ] `cargo test --lib -- plan::validate::tests` (compatibility gate)
- [ ] `cargo build --release`
- [ ] `cargo audit` (no unpatched CVEs in direct dependencies)
- [ ] All live `#[ignore]` integration tests pass (postgres + mysql + S3 + GCS-compat)

---

## Benchmark evidence (Phase 2)

Run `./dev/bench/run_bench.sh all` against the bench seed data and attach the
Markdown report to the release notes.

Required data points:

- [ ] §4.3 Compression profiles — wall time and output size for `none`/`fast`/`balanced`/`compact`
- [ ] §4.4 Row group targets — peak RSS and file size for 32/64/128/256 MB targets on `bench_wide`
- [ ] §4.5 Batch memory policies — wall time and RSS for `warn`/`auto_shrink`/no-cap on `bench_wide`
- [ ] §4.6 Quality uniqueness — RSS for capped vs uncapped on `bench_hc`

---

## Documentation review

- [ ] `docs/best-practices/` — all five guides reviewed, examples tested
- [ ] `docs/reference/config.md` — `compression_profile`, `parquet`, `quality`, `unique_max_entries` all documented
- [ ] `docs/reference/tuning.md` — `max_batch_memory_mb` / `on_batch_memory_exceeded` documented
- [ ] `CHANGELOG.md` — v0.5.x entry written

---

## Backward compatibility

- [ ] Old configs without `compression_profile` still work (`compression`/`compression_level` respected)
- [ ] Old configs without `parquet:` block still work (library default row group behavior)
- [ ] Old configs without `quality:` block still work (no quality checks run)
- [ ] Old configs without `tuning.max_batch_memory_mb` still work (no cap applied)

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
