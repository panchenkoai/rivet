#!/usr/bin/env bash
# Orchestrator for all regression matrix harnesses under dev/matrices/.
#
# Usage:
#   dev/matrices/run.sh [--tier=pr|nightly|release|all] [--matrix=cli,cfg,...]
#                       [--build] [--skip-compose]
#
# Examples:
#   ./run.sh --tier=pr              # cli + cfg + path (PR gate)
#   ./run.sh --matrix=cli           # CLI matrix only
#   ./run.sh --tier=release --build # full release gate, force rebuild
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
COMMON="$ROOT/_common"

TIER="all"
BUILD=0
SKIP_COMPOSE=0
MATRICES=()

usage() {
  cat <<EOF
Usage: dev/matrices/run.sh [OPTIONS]

Run regression matrix harnesses grouped under dev/matrices/.

Options:
  --tier=pr|nightly|release|all   Which matrices to run (default: all)
  --matrix=cli,cfg,...            Run only named matrices (overrides --tier)
  --build                         Force cargo build --release before run
  --skip-compose                  Assume docker services already up
  -h, --help                      Show this help

Matrices: cli, cfg, path, query, soak, cross_version

Tiers (see _common/compose_profiles.yaml):
  pr        cli, cfg, path
  nightly   pr + query, soak
  release   nightly + cross_version
  all       same as release
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier=*) TIER="${1#*=}"; shift ;;
    --matrix=*) IFS=',' read -ra MATRICES <<< "${1#*=}"; shift ;;
    --build) BUILD=1; shift ;;
    --skip-compose) SKIP_COMPOSE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

tier_list() {
  case "$1" in
    pr) echo "cli cfg path" ;;
    nightly) echo "cli cfg path query soak" ;;
    release|all) echo "cli cfg path query soak cross_version" ;;
    *)
      echo "Unknown tier '$1'" >&2
      exit 2
      ;;
  esac
}

if [[ ${#MATRICES[@]} -eq 0 ]]; then
  read -ra MATRICES <<< "$(tier_list "$TIER")"
fi

matrix_dir() {
  case "$1" in
    cli) echo "$ROOT/surface/cli" ;;
    cfg) echo "$ROOT/surface/cfg" ;;
    path) echo "$ROOT/execution/path" ;;
    query) echo "$ROOT/execution/query" ;;
    soak) echo "$ROOT/resources/soak" ;;
    cross_version) echo "$ROOT/compatibility/cross_version" ;;
    *)
      echo "Unknown matrix '$1'" >&2
      exit 2
      ;;
  esac
}

wait_postgres() {
  echo "Waiting for Postgres..."
  for _ in $(seq 1 30); do
    docker compose exec -T postgres pg_isready -U rivet >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "Postgres did not become ready in time" >&2
  exit 1
}

wait_mysql() {
  echo "Waiting for MySQL..."
  for _ in $(seq 1 30); do
    docker compose exec -T mysql mysqladmin ping -h localhost -uroot -privet >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "MySQL did not become ready in time" >&2
  exit 1
}

compose_up() {
  local profile="${1:-}"
  shift
  local -a services=("$@")
  if [[ $SKIP_COMPOSE -eq 1 ]]; then
    return 0
  fi
  (
    cd "$REPO"
    if [[ -n "$profile" ]]; then
      docker compose --profile "$profile" up -d "${services[@]}"
    else
      docker compose up -d "${services[@]}"
    fi
  )
}

ensure_services() {
  local need_pg=0 need_my=0 need_legacy=0
  for m in "${MATRICES[@]}"; do
    case "$m" in
      cli|cfg|cross_version) need_pg=1; need_my=1 ;;
      path|query|soak) need_pg=1 ;;
    esac
    [[ "$m" == cross_version ]] && need_legacy=1
  done

  if [[ $need_legacy -eq 1 ]]; then
    compose_up legacy \
      postgres postgres-12 postgres-13 postgres-14 postgres-15 \
      mysql mysql-57
  elif [[ $need_pg -eq 1 && $need_my -eq 1 ]]; then
    compose_up "" postgres mysql
  elif [[ $need_pg -eq 1 ]]; then
    compose_up "" postgres
  fi

  if [[ $need_pg -eq 1 ]]; then wait_postgres; fi
  if [[ $need_my -eq 1 ]]; then wait_mysql; fi
}

stage_all() {
  export STAGE_RIVET_BUILD=$BUILD
  for m in "${MATRICES[@]}"; do
    bash "$COMMON/lib/stage_rivet.sh" "$(matrix_dir "$m")" "$REPO"
  done
}

run_matrix() {
  local name="$1"
  local dir="$2"
  shift 2
  echo
  echo "=== $name ==="
  (cd "$dir" && "$@")
}

failures=0

ensure_services

if [[ $BUILD -eq 1 ]]; then
  (cd "$REPO" && cargo build --bin rivet --release)
fi

stage_all

for m in "${MATRICES[@]}"; do
  dir="$(matrix_dir "$m")"
  if [[ ! -d "$dir" ]]; then
    echo "Matrix directory missing: $dir (run dev/matrices/setup_links.sh)" >&2
    exit 2
  fi

  case "$m" in
    cli)
      bash "$COMMON/seed_pa_audit.sh"
      run_matrix "cli" "$dir" ./matrix.sh || failures=$((failures+1))
      run_matrix "cli (check_rc)" "$dir" ./check_rc.sh || failures=$((failures+1))
      ;;
    cfg)
      run_matrix "cfg" "$dir" ./matrix.sh || failures=$((failures+1))
      run_matrix "cfg (check_msg)" "$dir" ./check_msg.sh || failures=$((failures+1))
      ;;
    path)
      bash "$COMMON/seed_pa_audit.sh" --engines=postgres
      run_matrix "path" "$dir" ./matrix.sh || failures=$((failures+1))
      ;;
    query)
      bash "$COMMON/seed_pa_audit.sh" --engines=postgres
      run_matrix "query" "$dir" ./matrix.sh || failures=$((failures+1))
      ;;
    soak)
      if [[ ! -d "$dir/cfg" ]]; then
        run_matrix "soak (gen_fixtures)" "$dir" ./gen_fixtures.sh || failures=$((failures+1))
      fi
      bash "$COMMON/seed_pa_soak.sh"
      run_matrix "soak" "$dir" ./matrix.sh || failures=$((failures+1))
      ;;
    cross_version)
      bash "$COMMON/seed_pa_audit_all.sh"
      run_matrix "cross_version" "$dir" ./matrix.sh || failures=$((failures+1))
      run_matrix "cross_version (check_cross)" "$dir" ./check_cross.sh || failures=$((failures+1))
      ;;
  esac
done

echo
if (( failures > 0 )); then
  echo "FAILED: $failures matrix step(s) failed." >&2
  exit 1
fi
echo "OK: all requested matrices passed."
