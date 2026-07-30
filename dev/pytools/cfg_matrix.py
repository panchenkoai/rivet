#!/usr/bin/env python3
"""Port of `dev/cfg_matrix/gen_fixtures.sh` (1084 lines) + `dev/cfg_matrix/matrix.sh` (85).

Two scripts, one module, because they are two halves of one artifact: the
generator owns the canonical text of every YAML fixture under `cfg/`, and the
sweep runs `doctor` / `check` / `plan` against exactly those fixtures. Splitting
them would put the fixture inventory in one file and the thing that enumerates
it in another, which is how `gen_fixtures.sh` came to describe 83 fixtures while
its own README said 70.

    python3 -m dev.pytools.cfg_matrix gen [--check]
    python3 -m dev.pytools.cfg_matrix run [--clean] [--timeout=SECONDS]

`gen` is byte-for-byte faithful: all 84 generated files (83 YAML + the `c04`
sibling `.sql`) were diffed against the committed tree and are identical, and
`run` reproduces the same probe order, the same `%-44s  doctor=%-3s …` table and
the same log layout (`logs/<scenario>/<probe>/{cmd,stdout,stderr,exit_code}`)
that `check_msg.sh` reads. A fixture that differs by one byte changes which
cells pass, so the fixture bodies below are transcribed mechanically from the
heredocs rather than retyped.

Every place the shell turned a broken run into a passing one is marked
`DEVIATION:` at its site. They are:

1. **A failed write still reported "Generated 83 YAML fixtures".** `gen_fixtures.sh`
   set `-u` but never `-e` or `-o pipefail`, and `w()`'s `mkdir -p` + `cat > "$path"`
   were both unchecked — a read-only checkout, a full disk or an EPERM produced
   the success line and exit 0 with nothing (or half of something) written.
2. **`cat > "$path"` truncates the fixture before the body arrives.** The
   `generator > file` shape: a crash mid-write leaves a half fixture that the
   next reviewer's `git diff` reports as legitimate drift. Now `atomic_write`.
3. **The count could not fall.** `Generated $(find "$CFG" -name '*.yaml' | wc -l)`
   counts what is on DISK, not what was generated, so a fixture deleted from the
   generator kept inflating the number forever — and, worse, that orphan still ran
   in `matrix.sh` as a first-class scenario against a contract nobody maintained.
   The headline stays byte-identical; strays are now named on stderr, and
   `gen --check` fails on them.
4. **An empty or missing `cfg/` was a silent pass.** `find` printed its error to
   stderr, the `for` body never ran, and `matrix.sh` finished with
   "DONE.  0 scenarios across 3 probes captured in …" and exit 0 — the whole
   config surface unprobed, reported as done. Now `Fail` with exit 2.
5. **`for yaml in $(find … | sort)` was word-split and glob-expanded**, so a path
   containing a space became two bogus scenarios and one containing `[` or `*`
   became a different path or vanished. `$?` could not see it either: the status
   of that pipeline is `sort`'s, so a failing `find` read as success (bug class 2).
6. **A skipped `plan` left the PREVIOUS run's `plan/` transcript in place.** Only
   a `skipped` marker was added, so a scenario that no longer yields an export
   name kept satisfying `expected_msg.txt` against a stale `stdout`/`stderr`/
   `exit_code` from an older binary. The skip now clears the transcript, and a
   real probe clears a stale `skipped` marker.
7. **A log-directory failure surfaced as a fake probe exit code** (bug class 8).
   `run_probe` ran inside `$(…)`; when its `mkdir -p` failed, the unchecked
   `"$@" > "$dir/stdout"` redirection failed instead of the probe, and the
   function returned `1` — indistinguishable in the table, and in
   `exit_code`, from `rivet doctor` genuinely rejecting the config.
8. **No probe had a timeout.** A `doctor` blocking on an unreachable host hung the
   sweep indefinitely. Bounded by `--timeout` (default 300 s); a timeout is
   recorded, warned about, and makes the run exit non-zero — it must not read as
   one more ordinary non-zero rc among the 18 negative fixtures.
9. **`RIVET_BIN` pointing at a non-executable path was silently ignored** and the
   sweep ran `target/release/rivet` instead — the exact substitution an explicit
   override exists to prevent. The fallback is kept (faithful) but now says so.
10. **`[[ -x "$R" ]]` is true for a DIRECTORY.** A `dev/cfg_matrix/rivet/` directory
    passed the guard and all 249 probes then failed with rc=126, recorded as if
    the binary had rejected 83 configs. A regular file is now required.

Bug classes checked for and NOT present in these two scripts, so nothing was
changed on their account: a `case` with no default arm (neither script branches
on a value — but `main_cli` below validates its subcommand explicitly rather than
falling through to 0); `grep PATTERN fileA fileB` with two operands; a `0`
fallback feeding a threshold gate; `/usr/bin/time -l`; and `exit N` inside
`$(…)` — `run_probe` does run in a command substitution, but contains no `exit`,
so only the redirection hazard above (deviation 7) applies. The
`local x=$1 y="…${x}…"` bash-3.2 scope trap is also absent: all four `local`
sites declare a single name, and the two that read an earlier local (`dir` from
`sid`/`probe`) do so in a SEPARATE statement, which is safe.

`matrix.sh` is OBSERVATION mode — a non-zero probe rc is DATA, not a failure, and
that is preserved: negative fixtures are expected to fail and the sweep still
exits 0. `check_msg.sh` (ported in `dev/pytools/matrices.py`) is what turns the
captured transcripts into a verdict.
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:  # `python3 -m dev.pytools.cfg_matrix`
    from . import shell
except ImportError:  # `python3 dev/pytools/cfg_matrix.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
atomic_write, log, ok, bad, skip, warn = (
    shell.atomic_write,
    shell.log,
    shell.ok,
    shell.bad,
    shell.skip,
    shell.warn,
)

# `ROOT="$(cd "$(dirname "$0")" && pwd)"` in both scripts — the matrix directory,
# resolved absolute. Every path the originals print is absolute, and the printed
# paths are part of the output contract, so these stay absolute too.
MATRIX_DIR = ROOT / "dev" / "cfg_matrix"
CFG = MATRIX_DIR / "cfg"
LOGS = MATRIX_DIR / "logs"

# The three probes, in the order `matrix.sh` runs them. Why three:
#   doctor → source connect + preflight messages
#   check  → full config validation + DB reachability
#   plan   → export + destination validation; needs a valid export name
PROBES: tuple[str, str, str] = ("doctor", "check", "plan")

# `export PG_URL="${PG_URL:-…}"`: `:-` means an EMPTY value also takes the
# default, and the assignment is exported so the probes inherit it.
ENV_DEFAULTS: tuple[tuple[str, str], ...] = (
    ("PG_URL", "postgresql://rivet:rivet@127.0.0.1:5432/rivet"),
    ("MY_URL", "mysql://rivet:rivet@127.0.0.1:3306/rivet"),
    ("PG_PASSWORD", "rivet"),
    ("MY_PASSWORD", "rivet"),
)


# ── fixture inventory ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Fixture:
    """One generated file. `path` is relative to `cfg/`, or to the matrix
    directory itself when `sidecar` (the two `_*_url.txt` files the `url_file`
    fixtures point at, which are gitignored generated artifacts)."""

    path: str
    body: str
    sidecar: bool = False

    def dest(self, *, cfg_dir: Path, matrix_dir: Path) -> Path:
        return (matrix_dir if self.sidecar else cfg_dir) / self.path


def F(path: str, body: str) -> Fixture:
    return Fixture(path, body)


def S(path: str, body: str) -> Fixture:
    return Fixture(path, body, sidecar=True)


# Naming, from the generator's own header:
#   <group_letter><nn>_<short_label>.yaml
#   a = source-connection axis          e = edge configuration
#   b = TLS-mode axis                   f = multi-export / parallel / notifications
#   c = export mode / query / format    g = negative (should fail validate)
#   d = destination type
#
# Order is the bash's write order (a, b, c, d, e01-e10, f, g01-g17, e11-e21,
# c13-c14, g18, sidecars) — it does not affect the output, but keeping it makes
# the two files diffable against each other.
FIXTURES: tuple[Fixture, ...] = (
    # ── A. Source connection method × source_type ───────────────────────
    F("source/a01_pg_url.yaml", """\
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a01 }
"""),
    F("source/a02_pg_url_env.yaml", """\
source:
  type: postgres
  url_env: PG_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a02 }
"""),
    F("source/a03_pg_url_file.yaml", """\
source:
  type: postgres
  url_file: ../../cfg_matrix/_pg_url.txt
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a03 }
"""),
    F("source/a04_pg_structured_password.yaml", """\
source:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a04 }
"""),
    F("source/a05_pg_structured_password_env.yaml", """\
source:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: rivet
  password_env: PG_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a05 }
"""),
    F("source/a06_my_url.yaml", """\
source:
  type: mysql
  url: "mysql://rivet:rivet@127.0.0.1:3306/rivet"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a06 }
"""),
    F("source/a07_my_url_env.yaml", """\
source:
  type: mysql
  url_env: MY_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a07 }
"""),
    F("source/a08_my_url_file.yaml", """\
source:
  type: mysql
  url_file: ../../cfg_matrix/_my_url.txt
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a08 }
"""),
    F("source/a09_my_structured_password.yaml", """\
source:
  type: mysql
  host: 127.0.0.1
  port: 3306
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a09 }
"""),
    F("source/a10_my_structured_password_env.yaml", """\
source:
  type: mysql
  host: 127.0.0.1
  port: 3306
  user: rivet
  password_env: MY_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a10 }
"""),
    # ── B. TLS mode axis (PG + MySQL, every mode + invalid-cert escapes) ─────
    F("tls/b01_pg_tls_disable.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: disable }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b01 }
"""),
    F("tls/b02_pg_tls_require.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: require }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b02 }
"""),
    F("tls/b03_pg_tls_verify_ca.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-ca }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b03 }
"""),
    F("tls/b04_pg_tls_verify_full.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-full }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b04 }
"""),
    F("tls/b05_pg_tls_accept_invalid_certs.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: require, accept_invalid_certs: true }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b05 }
"""),
    F("tls/b06_my_tls_disable.yaml", """\
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: disable }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b06 }
"""),
    F("tls/b07_my_tls_require.yaml", """\
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: require }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b07 }
"""),
    F("tls/b08_my_tls_verify_ca.yaml", """\
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: verify-ca }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b08 }
"""),
    F("tls/b09_my_tls_verify_full.yaml", """\
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: verify-full }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b09 }
"""),
    F("tls/b10_pg_tls_accept_invalid_hostnames.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-full, accept_invalid_hostnames: true }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b10 }
"""),
    # ── C. Export mode × source of query × format ────────────────────────
    F("export/c01_full_query_parquet.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c01 }
"""),
    F("export/c02_full_query_csv.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: csv
    destination: { type: local, path: ./out/c02 }
"""),
    F("export/c03_full_table_shortcut.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c03 }
"""),
    F("export/c04_full_query_file.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query_file: query_c04.sql
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c04 }
"""),
    F("export/c05_incremental_parquet.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: id
    format: parquet
    destination: { type: local, path: ./out/c05 }
"""),
    F("export/c06_chunked_parquet.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/c06 }
"""),
    F("export/c07_chunked_with_checkpoint.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    chunk_checkpoint: true
    format: parquet
    destination: { type: local, path: ./out/c07 }
"""),
    F("export/c08_time_window_parquet.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: time_window
    time_column: created_at
    days_window: 7
    format: parquet
    destination: { type: local, path: ./out/c08 }
"""),
    F("export/c09_incremental_with_fallback.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: id
    format: parquet
    destination: { type: local, path: ./out/c09 }
"""),
    F("export/c10_chunked_with_chunk_count.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_count: 4
    format: parquet
    destination: { type: local, path: ./out/c10 }
"""),
    F("export/c11_chunked_by_days.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: created_at
    chunk_by_days: 1
    format: parquet
    destination: { type: local, path: ./out/c11 }
"""),
    F("export/c12_full_skip_empty.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE 1=0"
    mode: full
    skip_empty: true
    format: parquet
    destination: { type: local, path: ./out/c12 }
"""),
    # Sibling query file for c04
    F("export/query_c04.sql", """\
SELECT id, name FROM pa_audit
"""),
    # ── D. Destination types ─────────────────────────────────────
    F("destination/d01_dest_local.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/d01 }
"""),
    F("destination/d02_dest_stdout.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: stdout }
"""),
    F("destination/d03_dest_s3.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: rivet-cfg-matrix-test
      prefix: d03/
      region: us-east-1
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY
"""),
    F("destination/d04_dest_gcs.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: rivet-cfg-matrix-test
      prefix: d04/
      credentials_file: /tmp/gcs.json
"""),
    F("destination/d05_dest_azure.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: azure
      account_name: rivetcfgmatrix
      bucket: container
      prefix: d05/
"""),
    # ── E. Edge: drift / quality / compression / row-group / meta ────────────
    F("edge/e01_drift_warn.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: warn
    destination: { type: local, path: ./out/e01 }
"""),
    F("edge/e02_drift_continue.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: continue
    destination: { type: local, path: ./out/e02 }
"""),
    F("edge/e03_drift_fail.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: fail
    destination: { type: local, path: ./out/e03 }
"""),
    F("edge/e04_quality_row_count.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    quality:
      row_count_min: 1
      row_count_max: 1000
    destination: { type: local, path: ./out/e04 }
"""),
    F("edge/e05_quality_null_ratio_unique.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    quality:
      null_ratio_max: { name: 0.5 }
      unique_columns: [id]
    destination: { type: local, path: ./out/e05 }
"""),
    F("edge/e06_compression_profile_fast.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression_profile: fast
    destination: { type: local, path: ./out/e06 }
"""),
    F("edge/e07_compression_zstd_level.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: zstd
    compression_level: 9
    destination: { type: local, path: ./out/e07 }
"""),
    F("edge/e08_meta_columns_row_hash.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    meta_columns: { exported_at: true, row_hash: true }
    destination: { type: local, path: ./out/e08 }
"""),
    F("edge/e09_row_group_fixed_rows.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    parquet:
      row_group_strategy: fixed_rows
      row_group_rows: 500
    destination: { type: local, path: ./out/e09 }
"""),
    F("edge/e10_compression_profile_and_level.yaml", """\
# profile is supposed to take precedence over (compression + compression_level)
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: gzip
    compression_level: 3
    compression_profile: compact
    destination: { type: local, path: ./out/e10 }
"""),
    # ── F. Multi-export / parallel / notifications ─────────────────────
    F("multi/f01_two_exports.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f01_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f01_b }
"""),
    F("multi/f02_parallel_exports.yaml", """\
source: { type: postgres, url_env: PG_URL }
parallel_exports: true
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f02_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f02_b }
"""),
    F("multi/f03_parallel_export_processes.yaml", """\
source: { type: postgres, url_env: PG_URL }
parallel_export_processes: true
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f03_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f03_b }
"""),
    F("multi/f04_slack_webhook.yaml", """\
source: { type: postgres, url_env: PG_URL }
notifications:
  slack:
    webhook_url: "https://hooks.slack.example/notreal"
    on: [failure, schema_change]
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f04 }
"""),
    F("multi/f05_slack_webhook_env.yaml", """\
source: { type: postgres, url_env: PG_URL }
notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK_URL
    on: [failure]
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f05 }
"""),
    # ── G. Negative: should fail with a clear error ─────────────────────
    F("negative/g01_url_plus_url_env.yaml", """\
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
  url_env: PG_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g01 }
"""),
    F("negative/g02_url_plus_structured.yaml", """\
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
  host: 127.0.0.1
  user: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g02 }
"""),
    F("negative/g03_password_plus_password_env.yaml", """\
source:
  type: postgres
  host: 127.0.0.1
  user: rivet
  password: rivet
  password_env: PG_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g03 }
"""),
    F("negative/g04_query_plus_query_file.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    query_file: ../export/query_c04.sql
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g04 }
"""),
    F("negative/g05_query_plus_table.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    table: pa_audit
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g05 }
"""),
    F("negative/g06_missing_source.yaml", """\
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g06 }
"""),
    F("negative/g07_empty_exports.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports: []
"""),
    F("negative/g08_structured_missing_host.yaml", """\
source:
  type: postgres
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g08 }
"""),
    F("negative/g09_structured_missing_user.yaml", """\
source:
  type: postgres
  host: 127.0.0.1
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g09 }
"""),
    F("negative/g10_unknown_top_level_key.yaml", """\
source: { type: postgres, url_env: PG_URL }
mystery_key: 42
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g10 }
"""),
    F("negative/g11_unknown_export_key.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    nonsense_export_field: yes
    destination: { type: local, path: ./out/g11 }
"""),
    F("negative/g12_bad_export_mode.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: gibberish
    format: parquet
    destination: { type: local, path: ./out/g12 }
"""),
    F("negative/g13_bad_format.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: avro
    destination: { type: local, path: ./out/g13 }
"""),
    F("negative/g14_misplaced_tuning.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    batch_size: 100   # belongs under tuning:, not on the export root
    destination: { type: local, path: ./out/g14 }
"""),
    F("negative/g15_malformed_yaml.yaml", """\
source: { type: postgres, url_env: PG_URL
exports:
  - name: pa_audit
"""),
    F("negative/g16_query_file_traversal.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query_file: ../../../../etc/passwd
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g16 }
"""),
    F("negative/g17_incremental_no_cursor.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    format: parquet
    destination: { type: local, path: ./out/g17 }
"""),
    # ── Edge: tuning subgraph + cursor modes + compression variants ───────────
    F("edge/e11_tuning_source_level.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  tuning:
    profile: fast
    batch_size: 500
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e11 }
"""),
    F("edge/e12_tuning_export_overrides.yaml", """\
# Export-level tuning takes precedence over source-level. Pin so a refactor
# that flips the merge precedence is caught.
source:
  type: postgres
  url_env: PG_URL
  tuning:
    profile: balanced
    batch_size: 1000
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e12 }
    tuning:
      batch_size: 100
"""),
    F("edge/e13_tuning_batch_memory.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e13 }
    tuning:
      batch_size_memory_mb: 16
"""),
    F("edge/e14_tuning_throttle.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e14 }
    tuning:
      throttle_ms: 5
"""),
    F("edge/e15_compression_lz4.yaml", """\
# lz4 is in the enum but was not exercised by the original sweep — pin so
# a downstream parquet-writer refactor that drops a codec is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: lz4
    destination: { type: local, path: ./out/e15 }
"""),
    F("edge/e16_compression_none.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: none
    destination: { type: local, path: ./out/e16 }
"""),
    F("edge/e17_source_environment_production.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  environment: production
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e17 }
"""),
    F("edge/e18_source_environment_replica.yaml", """\
source:
  type: postgres
  url_env: PG_URL
  environment: replica
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e18 }
"""),
    F("edge/e19_incremental_cursor_coalesce.yaml", """\
# `coalesce` mode is what `cursor_fallback_column` requires — pin the happy
# path so a refactor that drops the COALESCE rewrite is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: id
    cursor_fallback_column: id
    incremental_cursor_mode: coalesce
    format: parquet
    destination: { type: local, path: ./out/e19 }
"""),
    F("edge/e20_max_file_size.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    max_file_size: "10MB"
    destination: { type: local, path: ./out/e20 }
"""),
    F("edge/e21_chunk_size_memory_mb.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size_memory_mb: 32
    format: parquet
    destination: { type: local, path: ./out/e21 }
"""),
    # ── Export: MySQL × time_window — only PG was covered before ──────────────
    F("export/c13_time_window_mysql.yaml", """\
source: { type: mysql, url_env: MY_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: time_window
    time_column: created_at
    days_window: 7
    format: parquet
    destination: { type: local, path: ./out/c13 }
"""),
    F("export/c14_chunked_mysql.yaml", """\
source: { type: mysql, url_env: MY_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/c14 }
"""),
    F("negative/g18_chunked_no_column.yaml", """\
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/g18 }
"""),
    # Sidecar files used by the url_file fixtures (a03 / a08). Gitignored: they are
    # generated artifacts, not reviewable source, so `--check` does not diff them.
    S("_pg_url.txt", """\
postgresql://rivet:rivet@127.0.0.1:5432/rivet
"""),
    S("_my_url.txt", """\
mysql://rivet:rivet@127.0.0.1:3306/rivet
"""),
)


# ── generation (gen_fixtures.sh) ───────────────────────────────────────────────
def generate_fixtures(*, cfg_dir: Path = CFG, matrix_dir: Path = MATRIX_DIR) -> list[Path]:
    """Write every fixture, in the generator's original order. Returns the paths.

    DEVIATION 1 + 2: `w()` was `mkdir -p "$(dirname …)"` followed by
    `cat > "$path"`, with neither status checked and no `set -e` anywhere in the
    script — a read-only checkout or a full disk still reached the
    "Generated … fixtures" line and exited 0. And the `>` truncated the target
    *before* the heredoc arrived, so an interrupted run left a half fixture that
    the next `git diff` reports as a legitimate edit. `atomic_write` raises on
    failure and renames a complete temp file into place, so every fixture on disk
    is always either the whole previous one or the whole new one.
    """
    written: list[Path] = []
    try:
        for f in FIXTURES:
            dest = f.dest(cfg_dir=cfg_dir, matrix_dir=matrix_dir)
            try:
                atomic_write(dest, f.body)
            except OSError as e:
                # An unwritable checkout is an EXPECTED condition, so it gets a
                # message naming the file rather than a traceback. Proven RED
                # against the bash: with `cfg/` mode 500 it printed "Generated 0
                # YAML fixtures under …" and exited 0 — the success line for a
                # run that wrote nothing whatsoever.
                raise Fail(f"cannot write fixture {dest}: {e.strerror or e}") from None
            written.append(dest)
    finally:
        # A raise inside `atomic_write` can leave its `<name>.tmp` behind. It would
        # not be matched by `*.yaml` so nothing downstream trips over it, but a
        # stray temp beside a reviewed fixture is confusing. Only paths this
        # module owns are swept — never a blanket `rglob("*.tmp")`.
        for f in FIXTURES:
            d = f.dest(cfg_dir=cfg_dir, matrix_dir=matrix_dir)
            d.with_suffix(d.suffix + ".tmp").unlink(missing_ok=True)
    return written


def owned_yaml() -> frozenset[str]:
    """The `cfg/`-relative YAML paths this module is the source of truth for."""
    return frozenset(f.path for f in FIXTURES if not f.sidecar and f.path.endswith(".yaml"))


def strays(*, cfg_dir: Path = CFG) -> list[Path]:
    """YAMLs on disk under `cfg/` that this generator does not produce.

    They matter because `matrix.sh` sweeps the DIRECTORY, not the inventory: an
    orphan runs as a first-class scenario forever, against a contract nobody
    maintains, and inflates the generator's own "Generated N" headline.
    """
    owned = owned_yaml()
    return [
        p
        for p in sorted(cfg_dir.rglob("*.yaml"), key=str)
        if p.relative_to(cfg_dir).as_posix() not in owned
    ]


def gen(*, cfg_dir: Path = CFG, matrix_dir: Path = MATRIX_DIR) -> int:
    """Regenerate every fixture. Idempotent; mirrors `gen_fixtures.sh`."""
    log(f"regenerating {len(FIXTURES)} cfg_matrix fixtures", tag="cfg_matrix")
    generate_fixtures(cfg_dir=cfg_dir, matrix_dir=matrix_dir)

    # Byte-identical headline. Faithfully counts what is on DISK (recursively,
    # as `find`), not what was generated — see the stray warning below.
    on_disk = sorted(cfg_dir.rglob("*.yaml"), key=str)
    print(f"Generated {len(on_disk)} YAML fixtures under {cfg_dir}", flush=True)

    # DEVIATION 3: the bash count could only ever rise. A fixture deleted from
    # the generator stayed on disk, kept being counted as "Generated", and kept
    # being swept by matrix.sh. Naming strays on stderr leaves the stdout
    # headline untouched while making the discrepancy impossible to miss.
    extra = strays(cfg_dir=cfg_dir)
    if extra:
        warn(
            f"{len(extra)} YAML under {cfg_dir} not generated by this module — "
            "counted above, and swept by `run` as real scenarios:"
        )
        for p in extra:
            warn(f"    {p.relative_to(cfg_dir).as_posix()}")
    return 0


def check_fixtures(*, cfg_dir: Path = CFG) -> int:
    """Diff the committed fixtures against this module; 1 on any drift.

    The fixtures are committed as plain files so reviewers read them directly,
    which means the generator and the tree can silently disagree. This is the
    guard that makes "byte-identical fixtures" enforceable instead of aspirational.
    The two `_*_url.txt` sidecars are skipped: they are gitignored generated
    artifacts, not reviewable source.
    """
    drift: list[str] = []
    for f in FIXTURES:
        if f.sidecar:
            continue
        dest = cfg_dir / f.path
        if not dest.is_file():
            drift.append(f"missing:  {f.path}")
        elif dest.read_text() != f.body:
            drift.append(f"differs:  {f.path}")
    for p in strays(cfg_dir=cfg_dir):
        drift.append(f"stray:    {p.relative_to(cfg_dir).as_posix()}")

    if drift:
        bad(f"{len(drift)} fixture(s) drifted from dev/pytools/cfg_matrix.py:")
        for line in drift:
            bad(f"    {line}")
        print(
            "      Reconcile: `python3 -m dev.pytools.cfg_matrix gen` (adopts this "
            "module's text),\n      or edit FIXTURES to adopt the tree.",
            file=sys.stderr,
        )
        return 1
    ok(f"{len(owned_yaml())} YAML fixtures match the generator byte for byte")
    return 0


# ── the sweep (matrix.sh) ──────────────────────────────────────────────────────
DEFAULT_PROBE_TIMEOUT = 300.0


def _executable(path: Path) -> bool:
    """DEVIATION 10: bash's `[[ -x "$R" ]]` is TRUE for a directory, so a
    `dev/cfg_matrix/rivet/` directory passed the "binary not found" guard and
    every one of the 249 probes then failed with rc=126 — recorded in
    `exit_code` exactly as if the binary had rejected 83 configs. A probe target
    has to be a regular file."""
    return path.is_file() and os.access(path, os.X_OK)


def resolve_binary(*, matrix_dir: Path = MATRIX_DIR) -> Path:
    """`$RIVET_BIN`, else the matrix-local copy, else the workspace build.

    The per-matrix binary copies were 14 MB of stale artifacts each before the
    `dev/` cleanup, so falling back to `target/{release,debug}/rivet` is the
    normal path now, not the exception.

    One cosmetic difference, deliberate: the bash's fallback was the literal
    `$ROOT/../../target/release/rivet`, so every `logs/*/*/cmd` recorded an
    unnormalized `dev/cfg_matrix/../../target/release/rivet`. These paths are
    already absolute and resolved, so `cmd` now holds a path a reviewer can
    copy and paste. It is the ONLY byte difference in the 993 captured log
    files (verified: everything else matches bash except rivet's own per-run
    `plan_id` / `created_at` / `expires_at`).
    """
    override = os.environ.get("RIVET_BIN")
    first = Path(override) if override else matrix_dir / "rivet"
    release = ROOT / "target" / "release" / "rivet"
    debug = ROOT / "target" / "debug" / "rivet"

    for cand in (first, release, debug):
        if _executable(cand):
            # DEVIATION 9: a `RIVET_BIN` that is not executable was silently
            # discarded and the sweep ran a DIFFERENT binary — the one
            # substitution an explicit override exists to prevent. The fallback
            # is kept (faithful), but it no longer happens quietly.
            if override and cand != first:
                warn(f"$RIVET_BIN={override} is not an executable file — using {cand}")
            return cand

    # The bash error names whatever `$R` held last, i.e. the debug path.
    raise Fail(
        f"rivet binary not found at {debug}",
        code=2,
        hint=(
            "Build: cargo build --bin rivet --release && "
            "cp target/release/rivet dev/cfg_matrix/rivet"
        ),
    )


# POSIX `[[:space:]]` in the C locale, minus `\n` (a line never contains one).
# Spelled out rather than using `\s`, which in Python also matches non-ASCII
# whitespace the awk original would have left alone.
_BLANK = r"[ \t\v\f\r]"
_EXPORT_NAME = re.compile(rf"^{_BLANK}*-{_BLANK}+name:{_BLANK}*")
_TRAILING_BLANK = re.compile(rf"{_BLANK}+$")


def first_export_name(yaml_path: Path) -> str:
    """The first export's `name:` value, for the `plan` probe's `-e` argument.

    A direct transcription of the awk program: match `- name:` at the start of a
    line, strip that marker, remove every single and double quote from what is
    left, strip trailing blanks, stop at the first hit. Deliberately no better
    than the original — it is not a YAML parser, and it is what decides which
    fixtures get a third probe at all (`g07`, whose `exports:` is `[]`, is the
    one scenario that skips `plan`). Making it smarter would silently change the
    scenario set and therefore which cells pass.
    """
    for line in yaml_path.read_text(errors="replace").splitlines():
        m = _EXPORT_NAME.match(line)
        if m:
            rest = line[m.end() :].replace('"', "").replace("'", "")
            return _TRAILING_BLANK.sub("", rest)
    return ""


@dataclass(frozen=True)
class ProbeResult:
    rc: str
    timed_out: bool


def run_probe(
    sid: str,
    probe: str,
    argv: Sequence[str],
    *,
    logs_dir: Path = LOGS,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> ProbeResult:
    """Run one probe, capturing the transcript `check_msg.sh` reads.

    Layout is the bash's: `logs/<scenario>/<probe>/{cmd,stdout,stderr,exit_code}`.

    DEVIATION 7 (bug class 8): the original ran this inside `$(…)` and left
    `mkdir -p` unchecked, so when the log directory could not be created the
    unchecked `"$@" > "$dir/stdout"` redirection failed *instead of the probe*
    and the function returned `1` — indistinguishable, in the table and in the
    `exit_code` file, from `rivet doctor` genuinely rejecting the config. Every
    write here raises instead.
    """
    d = logs_dir / sid / probe
    d.mkdir(parents=True, exist_ok=True)

    # `printf '%s ' "$@"` then `printf '\n'`: each argument followed by a space,
    # so the file ends `… --format json \n`. Kept byte-identical, trailing space
    # included, since it is a committed-log artifact reviewers compare by eye.
    atomic_write(d / "cmd", " ".join(argv) + " \n")

    p = shell.run(list(argv), timeout=timeout)

    atomic_write(d / "stdout", p.stdout)
    atomic_write(d / "stderr", p.stderr)
    atomic_write(d / "exit_code", f"{p.returncode}\n")
    # DEVIATION 6 (other half): a scenario that used to skip `plan` and now runs
    # it kept its stale `skipped` marker, so the log directory claimed both.
    (d / "skipped").unlink(missing_ok=True)

    # DEVIATION 8: `shell.run` reports its own timeout kill as 124 and appends
    # the reason to stderr, so the transcript says why. A timeout must be louder
    # than an ordinary non-zero rc — 18 of these 83 fixtures are *supposed* to
    # fail, so a hung probe would otherwise blend into the expected noise.
    timed_out = p.returncode == 124
    if timed_out:
        bad(f"{sid}/{probe}: exceeded the {timeout:g}s timeout")
    return ProbeResult(str(p.returncode), timed_out)


def mark_plan_skipped(sid: str, *, logs_dir: Path = LOGS) -> None:
    """Record that `plan` had no export name to probe with.

    DEVIATION 6: the bash only ADDED the `skipped` marker. The previous run's
    `plan/{stdout,stderr,exit_code}` stayed, so `expected_msg.txt` kept being
    enforced against a transcript from an older binary — a scenario that no
    longer produces a plan at all could go on passing its plan contract
    indefinitely. The transcript is cleared, so an absent probe reads as absent.
    """
    d = logs_dir / sid / "plan"
    d.mkdir(parents=True, exist_ok=True)
    atomic_write(d / "skipped", "no-export-name\n")
    for stale in ("cmd", "stdout", "stderr", "exit_code"):
        (d / stale).unlink(missing_ok=True)


def run_matrix(
    *,
    clean: bool = False,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    cfg_dir: Path = CFG,
    logs_dir: Path = LOGS,
    matrix_dir: Path = MATRIX_DIR,
) -> int:
    """Run `doctor` + `check` + `plan` against every fixture under `cfg/`.

    OBSERVATION mode, preserved: a non-zero probe rc is DATA (18 fixtures are
    negative by design) and the sweep still exits 0. Only a harness failure — a
    probe that timed out, i.e. never produced an answer at all — makes it exit
    non-zero. Turning the transcripts into a verdict is `check_msg.sh`'s job.
    """
    binary = resolve_binary(matrix_dir=matrix_dir)

    # `export PG_URL="${PG_URL:-…}"`: `:-` gives the default for unset AND empty.
    # Restored in `finally` so calling `main_cli` in-process does not leave the
    # caller's environment permanently seeded with matrix credentials.
    saved: dict[str, str | None] = {n: os.environ.get(n) for n, _ in ENV_DEFAULTS}
    for name, default in ENV_DEFAULTS:
        if not os.environ.get(name):
            os.environ[name] = default

    try:
        if clean:
            # Not in the bash. Stale transcripts under `logs/` are read by
            # check_msg.sh whether or not the scenario still exists, so a removed
            # fixture can keep "passing"; opt-in so the default stays faithful.
            log(f"removing {logs_dir}", tag="cfg_matrix")
            shell.rm_rf(logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)

        # DEVIATION 5: `for yaml in $(find … | sort)` was word-split on IFS and
        # then glob-expanded, so a path containing a space became two bogus
        # scenarios and one containing `[` or `*` became a different path or
        # vanished. `$?` could not catch it either — the status of that pipeline
        # is `sort`'s, so a failing `find` read as success (bug class 2).
        # Sorted by string, not by `Path`, to match `sort`'s byte order exactly
        # (verified identical over all 83 paths).
        yamls = sorted(cfg_dir.rglob("*.yaml"), key=str)

        # DEVIATION 4: with no fixtures the bash loop body never ran and the
        # script printed "DONE.  0 scenarios across 3 probes captured in …" and
        # exited 0 — the entire config surface unprobed, reported as done.
        if not yamls:
            raise Fail(
                f"no YAML fixtures found under {cfg_dir}",
                code=2,
                hint="Generate them: python3 -m dev.pytools.cfg_matrix gen",
            )

        log(f"{len(yamls)} scenarios × 3 probes against {binary}", tag="cfg_matrix")

        count = 0
        timeouts = 0
        for yaml in yamls:
            sid = yaml.name[: -len(".yaml")]  # `basename "$yaml" .yaml`
            count += 1

            doctor = run_probe(
                sid, "doctor", [str(binary), "doctor", "-c", str(yaml)],
                logs_dir=logs_dir, timeout=timeout,
            )
            check = run_probe(
                sid, "check", [str(binary), "check", "-c", str(yaml)],
                logs_dir=logs_dir, timeout=timeout,
            )
            timeouts += int(doctor.timed_out) + int(check.timed_out)

            export_name = first_export_name(yaml)
            if export_name:
                plan = run_probe(
                    sid,
                    "plan",
                    [
                        str(binary), "plan", "-c", str(yaml),
                        "-e", export_name, "--format", "json",
                    ],
                    logs_dir=logs_dir,
                    timeout=timeout,
                )
                plan_rc = plan.rc
                timeouts += int(plan.timed_out)
            else:
                plan_rc = "--"
                mark_plan_skipped(sid, logs_dir=logs_dir)
                skip(f"{sid}: no `- name:` in the YAML — plan not probed")

            # `printf '%-44s  doctor=%-3s  check=%-3s  plan=%-3s\n'`, trailing
            # padding included. The table is the script's DATA, so it stays on
            # stdout while every line above went to stderr.
            print(
                f"{sid:<44}  doctor={doctor.rc:<3}  check={check.rc:<3}  plan={plan_rc:<3}",
                flush=True,
            )

        print("", flush=True)
        print(f"DONE.  {count} scenarios across 3 probes captured in {logs_dir}", flush=True)

        if timeouts:
            bad(f"{timeouts} probe(s) never answered (timeout) — the sweep is incomplete")
            return 1
        return 0
    finally:
        for name, previous in saved.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


# ── CLI ────────────────────────────────────────────────────────────────────────
USAGE = """usage: cfg_matrix.py <command> [options]

  gen                 regenerate every fixture under dev/cfg_matrix/cfg/
                      (port of gen_fixtures.sh; idempotent)
      --check         write nothing — diff the committed fixtures against this
                      module and exit 1 on any drift or stray

  run                 capture doctor + check + plan for every fixture into
                      dev/cfg_matrix/logs/ (port of matrix.sh, observation mode)
      --clean         delete dev/cfg_matrix/logs/ first, so no stale transcript
                      from a removed scenario can satisfy expected_msg.txt
      --timeout=SEC   per-probe timeout in seconds (default 300)

  Environment: RIVET_BIN, PG_URL, MY_URL, PG_PASSWORD, MY_PASSWORD.
  Enforce the substring contract afterwards with dev/pytools/matrices.py."""


def _usage(message: str = "") -> int:
    """Usage on stderr with a non-zero code. Bug class 1 in spirit: a command
    this module does not understand must never look like a successful run."""
    if message:
        print(message, file=sys.stderr)
    print(USAGE, file=sys.stderr)
    return 2


def _parse_timeout(raw: str) -> float:
    """DEVIATION 8 (bug class 6): a failed parse must not fall back to `0`. A
    zero/negative per-probe budget would either fire instantly on every probe or,
    read as "no limit", restore the unbounded hang the timeout exists to prevent
    — a gate that cannot fire, dressed as a gate."""
    try:
        value = float(raw)
    except ValueError:
        raise Fail(f"--timeout expects a number of seconds, got {raw!r}", code=2) from None
    if value <= 0:
        raise Fail(f"--timeout must be positive, got {value:g}", code=2)
    return value


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        return _usage()
    if args[0] in ("-h", "--help", "help"):
        print(USAGE)
        return 0

    cmd, rest = args[0], args[1:]

    if cmd == "gen":
        check = False
        for a in rest:
            if a == "--check":
                check = True
            else:
                return _usage(f"unknown option for `gen`: {a}")
        return check_fixtures() if check else gen()

    if cmd == "run":
        clean = False
        timeout = DEFAULT_PROBE_TIMEOUT
        for a in rest:
            if a == "--clean":
                clean = True
            elif a.startswith("--timeout="):
                timeout = _parse_timeout(a.split("=", 1)[1])
            else:
                return _usage(f"unknown option for `run`: {a}")
        return run_matrix(clean=clean, timeout=timeout)

    return _usage(f"unknown command: {cmd}")


if __name__ == "__main__":
    shell.main(lambda: main_cli())
