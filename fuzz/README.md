# Fuzzing

Coverage-guided (libFuzzer) fuzzing of Rivet's **untrusted-input parsers**. The
contract each target enforces is *must not panic* — a garbled or hostile input
must produce a clean `Err`/`None`, never an `unwrap`/slice/overflow panic.

## Targets

| Target | Parser under test | Why it matters |
|--------|-------------------|----------------|
| `config_from_yaml` | `Config::from_yaml` | operator-authored config; a garbled file must error, not panic |
| `pg_test_decoding` | `parse_test_decoding` (PostgreSQL) | the richest text-parse surface (columns, typed values, arrays, intervals, timestamps); historically the most bug-prone (tz / datestyle / bytea rendering) |
| `mongo_resume_token` | `decode_resume_token` (MongoDB) | hex → BSON `Document::from_reader`, an untrusted binary-parse path |

Scope note: MySQL and SQL Server CDC decode the **binary** wire protocol inside
their driver crates (not Rivet code), and the Parquet path only *writes* from
already-typed Arrow — so those are not Rivet-owned untrusted-parse surfaces. The
three above are.

## Run it

Needs nightly Rust and `cargo-fuzz`:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz

# one target, 60 seconds, seeded from the committed corpus
cp -n fuzz/seeds/pg_test_decoding/* fuzz/corpus/pg_test_decoding/ 2>/dev/null || true
cargo +nightly fuzz run pg_test_decoding -- -max_total_time=60
```

A crash writes the reproducing input to `fuzz/artifacts/<target>/`; re-run it
with `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.

CI runs all three nightly (`.github/workflows/fuzz.yml`). The entry points live
behind the `fuzzing` cargo feature (`src/fuzz.rs`) and are never built into the
published crate.
