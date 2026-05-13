# Getting Started

Rivet exports tables from PostgreSQL or MySQL to Parquet (or CSV) files. Point it at a database, scaffold a config from your real tables, then run.

```bash
brew install panchenkoai/rivet/rivet
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run --config rivet.yaml --validate
```

The steps below walk through each command in detail.

## 1. Install

### Homebrew (macOS / Linux) — recommended

```bash
brew install panchenkoai/rivet/rivet
rivet --version
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/panchenkoai/rivet/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# macOS (Intel)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (arm64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/
```

```bash
rivet --version
```

### Docker — try without installing

```bash
# Check version
docker run --rm ghcr.io/panchenkoai/rivet:latest --version

# Run from the repo root using the bundled example config (no file copy needed)
mkdir -p output
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host.docker.internal:5432/db" \
  -v $(pwd)/examples/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml

# Or mount your own config
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host.docker.internal:5432/db" \
  -v $(pwd)/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml
```

> **Database on your laptop (not inside the container):** inside the container, `localhost` is the container itself — not your machine. Choose the approach for your OS:
>
> **Docker Desktop (macOS / Windows)** — use `host.docker.internal`:
> ```bash
> -e DATABASE_URL="mysql://user:pass@host.docker.internal:3306/db"
> ```
>
> **Linux — easiest: `--network host`** — shares the host network stack, so `127.0.0.1` works as-is:
> ```bash
> docker run --rm --network host \
>   -e DATABASE_URL="mysql://user:pass@127.0.0.1:3306/db" \
>   -v $(pwd)/examples/rivet.yaml:/config/rivet.yaml \
>   -v $(pwd)/output:/output \
>   ghcr.io/panchenkoai/rivet:latest \
>   run --config /config/rivet.yaml
> ```
>
> **Linux — alternative: `host-gateway`** (keeps container isolation):
> ```bash
> docker run --rm --add-host=host.docker.internal:host-gateway \
>   -e DATABASE_URL="mysql://user:pass@host.docker.internal:3306/db" \
>   ...
> ```
> Note: this requires MySQL to listen on `0.0.0.0` (not just `127.0.0.1`). If you still get **connection refused**, `--network host` is simpler.
>
> **Do not use the bridge gateway IP** (`172.17.0.1`) directly — MySQL's default `bind-address = 127.0.0.1` means it will refuse connections arriving on that interface.

### Build from source

Requires Rust 1.94+:

```bash
git clone https://github.com/panchenkoai/rivet.git
cd rivet
cargo build --release
sudo mv target/release/rivet /usr/local/bin/
rivet --version
```

From the registry, install the **`rivet-cli`** crate (binary name stays **`rivet`**): `cargo install rivet-cli`.

### Optional: shell completions

```bash
# Bash
rivet completions bash > ~/.local/share/bash-completion/completions/rivet

# Zsh
rivet completions zsh > ~/.zfunc/_rivet

# Fish
rivet completions fish > ~/.config/fish/completions/rivet.fish
```

## 2. Connect to your database

Rivet supports **PostgreSQL 12–16** and **MySQL 5.7 / 8.0**.

The recommended approach: put the connection URL in an environment variable and reference it from the config so no credentials are in the file:

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
```

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
```

For MySQL, use `type: mysql` and a `mysql://` URL. That's all you need to get started.

**Other connection styles** — inline `url:` (quick local test), `url_file:` (secrets mounted to disk), and structured `host` / `user` / `password_env` fields are also supported. Full reference: [reference/config.md § source](reference/config.md#source).

> **State database:** Rivet creates `.rivet_state.db` next to the config file (stores cursors, chunk checkpoints, run history). Add it to `.gitignore` if the folder is under version control.

## Walkthrough at a glance

The full basic workflow (`init` -> `doctor` -> `check` -> `run` -> `state`) in one GIF:

![Basic workflow](gifs/basic.gif)

Source: [docs/gifs/basic.tape](gifs/basic.tape). The step-by-step narrative continues below; individual steps have their own focused GIFs.

## 3. Scaffold a config with `rivet init` (optional)

If you prefer to start from your real tables instead of copying a template, use **`rivet init`**. It connects once, reads column lists and rough row estimates, and writes a YAML file with `url_env: DATABASE_URL` and suggested export modes.

![rivet init scaffolding a YAML from a live table](gifs/init-scaffold.gif)

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'

# Single table
rivet init --source-env DATABASE_URL --table orders -o my_export.yaml

# PostgreSQL: all tables/views in schema public
rivet init --source-env DATABASE_URL --schema public -o my_export.yaml

# JSON discovery artifact instead of YAML (ranked cursor/chunk candidates, sizes)
rivet init --source-env DATABASE_URL --schema public --discover -o discovery.json
```

Then review the file, adjust destinations and tuning, and continue with `rivet doctor` / `rivet check` below.

Full details: [reference/init.md](reference/init.md).

## 4. Verify connectivity

```bash
rivet doctor --config my_export.yaml
```

This checks that Rivet can connect to the source database and reach all configured destinations. Fix any `[FAIL]` items before proceeding.

## 5. Preflight check

```bash
rivet check --config my_export.yaml
```

This runs a dry-run analysis of each export: checks table existence, estimates row counts, detects index availability, and recommends a tuning profile.

![rivet check verdict block](gifs/check-verdict.gif)

Verdicts: `EFFICIENT` (indexed scan on sensible volume) · `ACCEPTABLE` (works but slower than ideal) · `DEGRADED` (full scan / missing index — see the `Suggestion:` line) · `UNSAFE` (estimated cost high enough to hurt production). `DEGRADED` and `UNSAFE` always carry a concrete, mode-aware suggestion.

## 6. Run your first export

```bash
rivet run --config my_export.yaml --validate
```

`--validate` verifies that the output file row count matches what was exported.

Add `--reconcile` to also compare with a `COUNT(*)` on the source:

```bash
rivet run --config my_export.yaml --validate --reconcile
```

### Optional: auditable execution with plan/apply

For CI/CD pipelines or pre-reviewed production runs, use the plan/apply workflow instead of `rivet run`:

![Sealed plan artifact, credential redaction (PA9), apply](gifs/plan-apply.gif)

```bash
# Generate a sealed execution plan (no data exported)
rivet plan --config my_export.yaml --format json -o plan.json

# Review plan.json, then execute
rivet apply plan.json
```

The plan artifact captures config, query fingerprints, and cursors at planning time. Plaintext `password:` values and `scheme://user:pass@` userinfo are stripped by [ADR-0005 PA9](adr/0005-plan-apply-contracts.md#pa9--artifact-credential-redaction-acr); references (`password_env` / `url_env` / `url_file`) are preserved so the apply environment can re-resolve them. `rivet apply` validates that nothing has changed before executing. See [reference/cli.md](reference/cli.md#rivet-plan) for details.

## 7. Inspect results

![Post-run inspection: state show, metrics, state files, state progression](gifs/inspect.gif)

```bash
# View export state (cursors for incremental exports)
rivet state show --config my_export.yaml

# View metrics history
rivet metrics --config my_export.yaml --last 10

# View produced files
rivet state files --config my_export.yaml

# Structured run journal: status, files, retries, quality issues, errors
rivet journal --config my_export.yaml --export orders

# Committed / verified boundaries (advisory; see ADR-0008)
rivet state progression --config my_export.yaml
```

`rivet state show` is empty after a `full` run (no cursor to record); `state progression` and `state files` populate for any run; `metrics` shows one row per run with `run_id`, rows, bytes, duration, peak RSS, and status. `rivet journal` shows a per-run event block — retries, quality issues, schema changes, and first-line error text — useful when a run fails or produces unexpected output.

## 8. Optional: reconcile and repair

For chunked exports run with `chunk_checkpoint: true`, Rivet can re-count every partition on the source and compare against the stored per-chunk row counts:

![Chunked export, drift detection, targeted repair](gifs/reconcile-repair.gif)

```bash
rivet reconcile --config my_export.yaml --export orders
```

If the report is dirty, `rivet repair --config my_export.yaml --export orders --execute` re-runs only the flagged chunk ranges — new files are written alongside the originals, and the committed boundary is not touched. See [reference/cli.md#rivet-repair](reference/cli.md#rivet-repair) and [ADR-0009](adr/0009-reconcile-and-repair-contracts.md).

## Next steps

- **Pilot guide (pick a path, then follow in order):** [pilot/README.md](pilot/README.md)
- **Demo quickstart** (pre-seeded 14-table fixture, ≈10 min): [pilot/demo-quickstart.md](pilot/demo-quickstart.md)
- **Full pilot walkthrough** — discovery → chunked → reconcile → repair → verified on your own data: [pilot/pilot-walkthrough.md](pilot/pilot-walkthrough.md)
- Choose the right export mode: [modes/](modes/)
- Configure your destination: [destinations/](destinations/)
- Tune for your workload: [reference/tuning.md](reference/tuning.md)
- Full config reference: [reference/config.md](reference/config.md)
- Auditable execution (plan/apply for CI/CD and pre-reviewed runs): [reference/cli.md](reference/cli.md#rivet-plan)
- Partition-level reconcile and targeted repair: [reference/cli.md](reference/cli.md#rivet-reconcile) · [ADR-0009](adr/0009-reconcile-and-repair-contracts.md)
- Committed / verified progression for operator dashboards: [ADR-0008](adr/0008-export-progression.md)
