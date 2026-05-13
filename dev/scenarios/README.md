# Rivet scenario configs (docker-compose stack)

Each YAML here is **one scenario surface**: one `source` and several `exports` that belong together for local / CI-style runs against `dev/postgres` + `dev/mysql` from `docker-compose.yaml`.

| File | Scenario |
|------|----------|
| `chunked_postgres_bench.yaml` | Chunked exports on `content_items`: parallel + checkpoint, serial, fatchunk, no-meta, balanced, **safe** profile, and `bench_content_seq` (chunked without checkpoint). Replaces former `dev/bench_chunked_{p4,seq,p2,p4_safe}.yaml`. |
| `time_window_postgres.yaml` | `mode: time_window` on `events` (7-day window). Replaces `dev/_uat_time_window.yaml`. |

Layout under `dev/`:

| Directory | Contents |
|-----------|----------|
| `dev/workbench/` | Compose-oriented rivet configs: PG/MySQL full & structured URLs, incremental (+ coalesce), type matrix, preflight demos, `sparse_chunk_demo.yaml`. |
| `dev/fixtures/` | Feature-regression YAML (`test_*.yaml`): compression, stdout, params, quality, permissions, toxiproxy, schema evolution, etc. |
| `dev/cloud/` | S3 / GCS destination samples + `run_*_export.sh` helpers. |
| `dev/scripts/` | `run_uat_smoke.sh`, `regenerate_docker_init_configs.sh`, `bench.sh`, toxiproxy/schema/permission runners, `update_homebrew_formula.sh`. |
| `dev/e2e/` | CI E2E matrix configs (separate from workbench). |
| `examples/` | Small copy-paste examples. |

**Regenerated scaffolds** (`rivet init` samples) are not committed. Run:

```bash
bash dev/scripts/regenerate_docker_init_configs.sh
```

to populate `dev/init_generated/` locally (directory is gitignored).
