# Getting Started

## 1. Install

**From source (requires Rust 1.94+):**

```bash
cargo install --git https://github.com/andriipanchenko/rivet.git
```

**From a local clone:**

```bash
git clone https://github.com/andriipanchenko/rivet.git
cd rivet
cargo install --path .
```

Verify the installation:

```bash
rivet --version
```

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
