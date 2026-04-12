# Getting Started

## 1. Install

### Homebrew (macOS / Linux) — recommended

```bash
brew tap panchenkoai/rivet
brew update
brew install rivet
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

# Run an export (mount your config and output directory)
docker run --rm \
  -v $(pwd)/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml

# Pass credentials via environment variables
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host:5432/db" \
  -v $(pwd)/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml
```

> To connect to a database on your host machine, use `host.docker.internal` instead of `localhost` in the connection URL.

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

Rivet supports PostgreSQL and MySQL. The simplest config uses a connection URL:

```yaml
source:
  type: postgres
  url: "postgresql://user:password@host:5432/dbname"
```

For MySQL:

```yaml
source:
  type: mysql
  url: "mysql://user:password@host:3306/dbname"
```

**Avoid plaintext passwords.** Use an environment variable instead:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL    # reads from $DATABASE_URL at runtime
```

Or use structured fields:

```yaml
source:
  type: postgres
  host: db.example.com
  port: 5432
  user: rivet_reader
  password_env: DB_PASSWORD   # reads from $DB_PASSWORD
  database: production
```

## 2.5 Scaffold a config with `rivet init` (optional)

If you prefer to start from your real tables instead of copying a template, use **`rivet init`**. It connects once, reads column lists and rough row estimates, and writes a YAML file with `url_env: DATABASE_URL` and suggested export modes.

```bash
# Single table
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
rivet init --source "$DATABASE_URL" --table orders -o my_export.yaml

# PostgreSQL: all tables/views in schema public
rivet init --source "$DATABASE_URL" --schema public -o my_export.yaml

# MySQL: all tables/views in the database from the URL
rivet init --source 'mysql://user:pass@localhost:3306/mydb' -o my_export.yaml
```

Then review the file, adjust destinations and tuning, and continue with `rivet doctor` / `rivet check` below.

Full details: [reference/init.md](reference/init.md).

## 3. Verify connectivity

```bash
rivet doctor --config my_export.yaml
```

This checks that Rivet can connect to the source database and reach all configured destinations. Fix any `[FAIL]` items before proceeding.

## 4. Preflight check

```bash
rivet check --config my_export.yaml
```

This runs a dry-run analysis of each export: checks table existence, estimates row counts, detects index availability, and recommends a tuning profile.

## 5. Run your first export

```bash
rivet run --config my_export.yaml --validate
```

`--validate` verifies that the output file row count matches what was exported.

Add `--reconcile` to also compare with a `COUNT(*)` on the source:

```bash
rivet run --config my_export.yaml --validate --reconcile
```

## 6. Inspect results

```bash
# View export state (cursors for incremental exports)
rivet state show --config my_export.yaml

# View metrics history
rivet metrics --config my_export.yaml --last 10

# View produced files
rivet state files --config my_export.yaml
```

## Next steps

- Choose the right export mode: [modes/](modes/)
- Configure your destination: [destinations/](destinations/)
- Tune for your workload: [reference/tuning.md](reference/tuning.md)
- Full config reference: [reference/config.md](reference/config.md)
