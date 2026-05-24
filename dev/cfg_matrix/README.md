# YAML config matrix — coverage + regression guard

Sister of `dev/cli_matrix/`. That one stresses the CLI surface (flag
combinations, exit codes, error messages per subcommand); this one
stresses the **YAML configuration surface** — every connection method,
every TLS mode, every export shape, every destination type, every
known-invalid combination — and observes how the binary handles them.

## Why

The CLI matrix found behavior that exit codes alone hid (double-printed
warnings, `--param X was not referenced` firing on legitimate uses).
The config matrix found a different class:

- **MySQL+TLS panicked** with a cryptic message from the `mysql` crate
  because the binary was built without a TLS feature on that crate.
  Postgres + same config worked. (`b07`/`b08`/`b09`)
- **`mode: chunked` + `table:` shortcut** silently auto-resolved
  `chunk_column` from the primary key with no operator-visible signal,
  because the log line was `info!` and the default log level is `warn`.
  A typo in `chunk_column:` fell back to PK without warning. (`g18`)
- **`query_file: ../../../../etc/passwd`** passed `rivet check` and
  `rivet doctor` and only failed at plan time. (`g16`)
- **`rivet check` ignored destination credentials.** A config with
  `AWS_ACCESS_KEY_ID` unset passed `check` (rc=0) and then exploded
  on `run`. (`d03`)

Each of these survived because the existing test suite probed YAML
parsing in isolation, never end-to-end against the actual binary.

## Layout

```
cfg/source/         — connection method × source type (10 files)
cfg/tls/            — TLS mode × source type + cert-validation flags (10)
cfg/export/         — export mode × query|query_file|table × format (12)
cfg/destination/    — local / stdout / s3 / gcs / azure (5)
cfg/edge/           — drift policy / quality / compression / row-group (10)
cfg/multi/          — multi-export / parallel / notifications (5)
cfg/negative/       — every documented invalid combo (18)
```

70 YAMLs total; each runs through 3 probes (`doctor`, `check`, `plan`)
into `logs/<scenario>/<probe>/{stdout,stderr,exit_code,cmd}`.

## Running

```bash
docker compose up -d postgres mysql
cargo build --bin rivet --release
cp target/release/rivet dev/cfg_matrix/rivet
cd dev/cfg_matrix
./matrix.sh          # ~30s, 70 × 3 = 210 invocations
./check_msg.sh       # enforces expected_msg.txt contract
```

`gen_fixtures.sh` regenerates the YAMLs from the canonical source —
re-run it if you add a new axis.

## Regression contract

`expected_msg.txt` pins the substrings that must appear (`+`),
must not appear (`-`), or must appear exactly N times (`=N`) in
`stderr`/`stdout` per (scenario, probe). The same operators
the cli_matrix guard uses — see `dev/cli_matrix/expected_msg.txt`
for the same format applied to the CLI surface.

Adding a new YAML axis:

1. Add a YAML under the right `cfg/<group>/` subdir or extend
   `gen_fixtures.sh`.
2. Run `./matrix.sh` to capture behavior.
3. Inspect `logs/<new_id>/` to see what the binary actually does.
4. Add pinning entries to `expected_msg.txt` for the messages
   that constitute the user-facing contract (errors, hints, key WARNs).
5. Re-run `./check_msg.sh` to confirm it passes.
