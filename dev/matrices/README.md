# Test matrices

Regression guards that drive the **release binary** against docker fixtures
and diff captured artifacts against committed baselines.

For Rust integration tests (`tests/live_*.rs`), see
[docs/reference/testing.md](../../docs/reference/testing.md). For the operational
coverage map, see [docs/reliability-matrix.md](../../docs/reliability-matrix.md).

## Taxonomy

Matrices are grouped by **contract layer** — what they pin — not by technology.

| Layer | Matrix | Contract | Baseline format |
|-------|--------|----------|-----------------|
| **Surface** | [cli](../cli_matrix/) | CLI exit codes + messages | `expected_rc.txt`, `expected_msg.txt` |
| **Surface** | [cfg](../cfg_matrix/) | YAML probe behavior (doctor/check/plan) | `expected_msg.txt` |
| **Execution** | [path](../path_matrix/) | File layout + row accounting | `expected/*.layout`, `expected/*.summary` |
| **Execution** | [query](../query_matrix/) | PG `EXPLAIN` plan shape | `expected/*.plan` |
| **Resources** | [soak](../soak_matrix/) | Perf / memory bounds | `expected/*.thresholds` |
| **Compatibility** | [cross_version](../cross_version_matrix/) | rc agreement across DB versions | `check_cross.sh` |
| **Compatibility** | [legacy](../legacy/) | Full e2e per DB version (manual) | e2e assertions |

Directory layout:

```
dev/matrices/
├── README.md                 ← you are here
├── run.sh                    ← orchestrator (--tier=pr|nightly|release)
├── setup_links.sh            ← create symlinks under surface/, execution/, …
├── _common/                  ← shared fixtures, seeds, lib scripts
├── surface/cli  → cli_matrix
├── surface/cfg  → cfg_matrix
├── execution/path  → path_matrix
├── execution/query → query_matrix
├── resources/soak  → soak_matrix
└── compatibility/cross_version → cross_version_matrix
```

Canonical matrix sources remain at `dev/<name>_matrix/` so existing paths and
CI steps keep working. The grouped symlinks are the navigation layer.

## CI tiers

| Tier | Matrices | Trigger |
|------|----------|---------|
| **PR** | cli, cfg, path | every push / PR |
| **Nightly** | + query, soak | cron (planned) |
| **Release** | + cross_version | before tag |
| **Manual** | legacy full matrix, cloud smoke | operator |

```bash
# PR gate (same as CI matrices-pr job)
dev/matrices/run.sh --tier=pr

# Single matrix
dev/matrices/run.sh --matrix=cli

# Release gate with rebuild
dev/matrices/run.sh --tier=release --build
```

## Quick start

```bash
# One-time: create navigation symlinks
bash dev/matrices/setup_links.sh

# Build + run PR tier
cargo build --bin rivet --release
dev/matrices/run.sh --tier=pr --build
```

Each matrix can still be run standalone — see its README under `dev/<name>_matrix/`.

## Shared infrastructure (`_common/`)

| Path | Purpose |
|------|---------|
| `fixtures/pa_audit_*.sql` | 30-row table for surface/execution matrices |
| `fixtures/pa_soak_pg.sql` | 10k-row table for soak matrix |
| `seed_pa_audit.sh` | Seed primary PG and/or MySQL |
| `seed_pa_audit_all.sh` | Seed all PG 12–16 + MySQL 5.7/8.0 |
| `seed_pa_soak.sh` | Seed pa_soak on primary PG |
| `lib/check_msg.sh` | Substring contract checker (+ / - / =N) |
| `lib/extract_summary.sh` | Normalize summary.json accounting |
| `lib/normalize.sh` | Strip timestamps from file listings |
| `lib/stage_rivet.sh` | Copy release binary into a matrix dir |
| `compose_profiles.yaml` | Which docker services each matrix needs |

## Adding a scenario

1. Pick the matrix whose **contract layer** matches what you want to pin.
2. Add a fixture under that matrix's `cfg/` (or extend `gen_fixtures.sh`).
3. Run the matrix; inspect `logs/<id>/`.
4. Promote baselines into `expected/` or `expected_*.txt`.
5. Document intentional changes in `CHANGELOG.md`.
6. If the matrix is new, add a row to this README and `compose_profiles.yaml`.

## Scenario ID convention

```
<prefix><NN>_<axis>_<variant>

q01_full_scan           query matrix
p03_chunked_multifile   path matrix
a08_my_url_file         cfg/source axis
pg_run_full             cli matrix
s02_chunked_10k         soak matrix
```
