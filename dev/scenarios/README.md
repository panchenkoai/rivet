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
| `dev/cloud/` | S3 / GCS destination samples; the export helpers are `python3 -m dev.pytools.cloud_exports {s3,gcs,gcs-fake}`. |
| `dev/pytools/dev_scripts.py` | `python3 -m dev.pytools.dev_scripts` — `uat-smoke`, `regen-docker-configs`, `bench`, `live`, `permissions`, `retry-toxiproxy`, `schema-evolution`, `setup-toxiproxy`; the Homebrew formula updater is `python3 -m dev.pytools.homebrew_formula`. |
| `dev/e2e/` | CI E2E matrix configs (separate from workbench). |
| `examples/` | Small copy-paste examples. |

**Regenerated scaffolds** (`rivet init` samples) are not committed. Run:

```bash
python3 -m dev.pytools.dev_scripts regen-docker-configs
```

to populate `dev/init_generated/` locally (directory is gitignored).
