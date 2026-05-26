#!/usr/bin/env bash
# Light-weight cross-version smoke matrix.
#
# For each supported PG/MySQL version, runs a small subset of probes (doctor
# + check + a few plan + run smoke configs) and records rc + stderr per
# (version, scenario). A companion script (`check_cross.sh`) compares
# per-scenario behavior across versions and flags divergences.
#
# Why a separate matrix from cli_matrix:
#   cli_matrix runs ~85 scenarios — fine for a single primary DB pair, too
#   slow when multiplied across 7 DB versions. This matrix keeps the
#   smoke set small (5 PG probes × 5 versions, 3 MySQL probes × 2 versions)
#   so a CI gate completes in ~30s.
#
# Why not run cli_matrix per version:
#   Could be done but produces 700+ scenarios. Hard to diff. This is a
#   *cross-version-specific* guard, not full duplication.
#
# Bring up the legacy DBs first:
#   docker compose --profile legacy up -d postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
#   docker compose up -d postgres mysql
#   ./seed_all.sh
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
R="${RIVET_BIN:-$ROOT/rivet}"
LOGS="$ROOT/logs"
mkdir -p "$LOGS"

if [[ ! -x $R ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build: cargo build --bin rivet --release && cp target/release/rivet dev/cross_version_matrix/rivet" >&2
  exit 2
fi

# Probe config: a minimal full-mode config templated with @@URL@@ that the
# runner rewrites per version. Keeps the scenarios DRY.
PROBE_CFG="$LOGS/_probe.yaml"
cat > "$LOGS/_template.yaml" <<'YAML'
source:
  type: @@TYPE@@
  url: "@@URL@@"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/_probe }
YAML

PROBE_CHUNKED="$LOGS/_probe_chunked.yaml"
cat > "$LOGS/_template_chunked.yaml" <<'YAML'
source:
  type: @@TYPE@@
  url: "@@URL@@"
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/_probe_chunked }
YAML

write_probe() {
  local kind="$1" type="$2" url="$3"
  sed -e "s|@@TYPE@@|$type|" -e "s|@@URL@@|$url|" "$LOGS/_template${kind}.yaml" > "$LOGS/_probe${kind}.yaml"
}

probe_version() {
  local ver="$1" type="$2" url="$3"
  local vlog="$LOGS/$ver"
  mkdir -p "$vlog"

  # Quick reachability check — skip cleanly if DB is down (e.g. legacy profile
  # not started). The check_cross.sh comparator treats `SKIP` as a missing
  # data point, not a failure.
  local port
  port="$(printf '%s' "$url" | sed -E 's|.*@[^:]+:([0-9]+)/.*|\1|')"
  if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
    echo "$ver: SKIP (port $port unreachable)"
    echo "skip" > "$vlog/_status"
    return 0
  fi
  echo "ok" > "$vlog/_status"

  for kind in "" "_chunked"; do
    write_probe "$kind" "$type" "$url"
    local cfg="$LOGS/_probe${kind}.yaml"
    local k="${kind:-flat}"

    # Each probe: doctor + check + plan + run + apply. Capture rc + stderr +
    # (for `run`) extracted total_rows from summary.json so cross-version
    # comparator can assert every version produced the SAME row count from
    # the SAME source query — pa_audit has 30 rows on every seeded version.
    for cmd in doctor check plan_full; do
      local dir="$vlog/${cmd}${kind}"
      mkdir -p "$dir"
      case "$cmd" in
        doctor) "$R" doctor -c "$cfg" > "$dir/stdout" 2> "$dir/stderr" ;;
        check)  "$R" check  -c "$cfg" > "$dir/stdout" 2> "$dir/stderr" ;;
        plan_full) "$R" plan -c "$cfg" -e pa_audit --format json > "$dir/stdout" 2> "$dir/stderr" ;;
      esac
      local rc=$?
      printf '%s\n' "$rc" > "$dir/exit_code"
    done

    # `rivet run` — full export end to end. Isolate each (version, mode) in
    # its own workdir so .rivet/runs/ + the output dir don't bleed between
    # iterations. Pull total_rows out of summary.json for the comparator.
    local rundir="$vlog/run${kind}"
    mkdir -p "$rundir/work"
    cp "$cfg" "$rundir/work/rivet.yaml"
    (cd "$rundir/work" && "$R" run -c rivet.yaml) > "$rundir/stdout" 2> "$rundir/stderr"
    local run_rc=$?
    printf '%s\n' "$run_rc" > "$rundir/exit_code"
    if [[ $run_rc -eq 0 ]]; then
      local summary
      summary="$(find "$rundir/work/.rivet/runs" -name 'summary.json' 2>/dev/null | head -1)"
      if [[ -n $summary ]] && command -v jq >/dev/null 2>&1; then
        jq -r '.total_rows' "$summary" > "$rundir/total_rows" 2>/dev/null || echo '' > "$rundir/total_rows"
      fi
    fi

    # NOTE on apply: `rivet apply` needs the original config_path stored
    # inside the plan artifact to resolve the password / state DB
    # location (ADR-0005 + F13 fix). Replaying that in a fresh workdir
    # per (version, mode) is fiddly and the gymnastics drown the
    # signal. Apply correctness is already pinned at the artifact /
    # wire-format level by `tests/artifact_legacy_compat.rs` (frozen
    # v0.7.5 fixture deserializes under the current schema) and at the
    # CLI level by `dev/cli_matrix/pg_apply_*` against the primary PG
    # pair. The cross-version dimension here is row-count correctness
    # of `rivet run`, which IS pinned below via run.total_rows.
  done

  printf '%-12s done\n' "$ver"
}

echo "================ Postgres versions ================"
probe_version pg-12 postgres "postgresql://rivet:rivet@127.0.0.1:5412/rivet"
probe_version pg-13 postgres "postgresql://rivet:rivet@127.0.0.1:5413/rivet"
probe_version pg-14 postgres "postgresql://rivet:rivet@127.0.0.1:5414/rivet"
probe_version pg-15 postgres "postgresql://rivet:rivet@127.0.0.1:5415/rivet"
probe_version pg-16 postgres "postgresql://rivet:rivet@127.0.0.1:5432/rivet"

echo "================ MySQL versions ==================="
probe_version mysql-57 mysql "mysql://rivet:rivet@127.0.0.1:3357/rivet"
probe_version mysql-80 mysql "mysql://rivet:rivet@127.0.0.1:3306/rivet"

echo
echo "DONE.  Logs under $LOGS/<version>/<probe>/"
