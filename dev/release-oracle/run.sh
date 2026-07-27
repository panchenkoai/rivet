#!/usr/bin/env bash
# ── Rivet Release Oracle — driver ───────────────────────────────────────────
# Automates the manual dogfood into a go/no-go gate. Reads matrix.yaml, brings up
# each pinned engine version, seeds it from the canonical seed, and runs every
# scenario against the local object-store fakes (MinIO/fake-gcs/Azurite), then the
# BigQuery golden stage. GREEN everywhere ⇒ releasable.
#
# Usage:
#   run.sh                       # full gate (local stage + BigQuery if creds set)
#   run.sh --engines postgres,mysql   # subset
#   run.sh --no-cloud            # local stage only (skip BigQuery)
#   run.sh --keep                # leave engine containers up (debug)
#   run.sh --bless-bigquery-golden     # re-capture the BQ golden (intentional)
#
# Every check is PASS only when it RAN and MATCHED; a down service / absent cred is
# SKIP (never a silent pass). Exit 0 iff no non-skipped cell failed.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
MATRIX="$HERE/matrix.yaml"
RIVET="${RIVET_BIN:-$ROOT/target/release/rivet}"
WORK="$(mktemp -d)"
NET="rivet-oracle-net"
trap 'cleanup' EXIT

# ── result ledger ──
declare -a RESULTS   # "engine|version|scenario|store|STATUS|detail"
add() { RESULTS+=("$1|$2|$3|$4|$5|${6:-}"); }
RED=0

log()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
bad()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*"; RED=1; }
skip() { printf '\033[1;33m  ⊘ %s\033[0m\n' "$*"; }

cfg() { python3 "$HERE/lib/cfg.py" "$MATRIX" "$@"; }

cleanup() {
  [ "${KEEP:-0}" = 1 ] && return
  for c in $(docker ps -aq --filter "name=rivet-oracle-eng-" 2>/dev/null); do docker rm -f "$c" >/dev/null 2>&1; done
  rm -rf "$WORK"
}

# ── options ──
ENGINES_FILTER=""; NO_CLOUD=0; KEEP=0; BLESS=0; BLESS_VERDICTS=0; BLESS_DUCKDB=0; BLESS_CDC=0
while [ $# -gt 0 ]; do case "$1" in
  --engines) ENGINES_FILTER="$2"; shift 2;;
  --no-cloud) NO_CLOUD=1; shift;;
  --keep) KEEP=1; shift;;
  --bless-bigquery-golden) BLESS=1; shift;;
  --bless-local) BLESS_VERDICTS=1; BLESS_DUCKDB=1; NO_CLOUD=1; shift;;   # verdicts + duckdb-type goldens
  --bless-cdc) BLESS_CDC=1; shift;;                                       # cdc state-snapshot golden
  *) echo "unknown arg: $1"; exit 2;;
esac; done
export BLESS_VERDICTS BLESS_DUCKDB BLESS_CDC

[ -x "$RIVET" ] || { echo "rivet binary not found at $RIVET (build --release or set RIVET_BIN)"; exit 2; }
command -v duckdb >/dev/null || { echo "duckdb not on PATH (needed for the integrity oracle)"; exit 2; }

# ── local object stores (reuse the repo compose) ──
AZURITE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
start_stores() {
  log "Local object stores (MinIO / fake-gcs / Azurite)"
  ( cd "$ROOT" && docker compose up -d minio fake-gcs azurite >/dev/null 2>&1 )
  sleep 3
  # MinIO bucket via mc
  docker run --rm --network host --entrypoint sh minio/mc:latest -c \
    "mc alias set o http://127.0.0.1:9000 minioadmin minioadmin >/dev/null 2>&1 && mc mb -p o/rivet-oracle >/dev/null 2>&1; true" >/dev/null 2>&1 || true
  # fake-gcs bucket via the JSON API (404 on upload otherwise)
  curl -s -X POST "http://127.0.0.1:4443/storage/v1/b?project=oracle" \
    -H "Content-Type: application/json" -d '{"name":"rivet-oracle"}' >/dev/null 2>&1 || true
  # Azurite container via az CLI (best-effort; azure cell SKIPs if this is absent)
  command -v az >/dev/null && az storage container create --name rivet-oracle \
    --connection-string "$AZURITE_CONN" >/dev/null 2>&1 || true
  ok "stores up (bucket/container rivet-oracle: minio $(_store_up s3 && echo ✓||echo ✗) gcs $(_store_up gcs && echo ✓||echo ✗) azure $(_store_up azure && echo ✓||echo ✗))"
}

# ── per engine×version bring-up ──
# echoes the resolved connection URL on success, empty on skip.
bring_up() {
  local eng=$1 tag=$2 image=$3 port=$4
  # own line — macOS bash 3.2 expands a same-line ${eng}/${tag} against the ENCLOSING
  # scope (the leftover main-loop values), so the BQ stage's `bring_up postgres bq …`
  # mis-named the container after the last engine. Split so it sees the just-set args.
  local name="rivet-oracle-eng-${eng}-${tag//./_}"
  docker rm -f "$name" >/dev/null 2>&1 || true
  case "$eng" in
    postgres) docker run -d --name "$name" -e POSTGRES_USER=rivet -e POSTGRES_PASSWORD=rivet -e POSTGRES_DB=rivet -p "$port:5432" "$image" >/dev/null 2>&1 ;;
    mysql)    docker run -d --name "$name" -e MYSQL_ROOT_PASSWORD=rivet -e MYSQL_DATABASE=rivet -e MYSQL_USER=rivet -e MYSQL_PASSWORD=rivet -p "$port:3306" "$image" >/dev/null 2>&1 ;;
    mssql)    docker run -d --name "$name" -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=Rivet_Passw0rd!" -p "$port:1433" "$image" >/dev/null 2>&1 ;;
    mongo)    docker run -d --name "$name" -p "$port:27017" "$image" >/dev/null 2>&1 ;;
  esac || { skip "$eng:$tag could not start ($image)"; echo ""; return; }
  # wait healthy (best-effort, ~90s)
  local url; url="$(cfg url "$eng" | sed "s/%PORT%/$port/")"
  for i in $(seq 1 45); do
    case "$eng" in
      postgres) docker exec "$name" pg_isready -U rivet >/dev/null 2>&1 && break;;
      mysql)    docker exec "$name" mysqladmin ping -urivet -privet >/dev/null 2>&1 && break;;
      mssql)    docker exec "$name" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -Q 'SELECT 1' >/dev/null 2>&1 && break;;
      mongo)    docker exec "$name" mongosh --quiet --eval 'db.runCommand({ping:1})' >/dev/null 2>&1 && break;;
    esac; sleep 2
  done
  # mssql needs the rivet DB created
  [ "$eng" = mssql ] && docker exec "$name" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -Q "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet" >/dev/null 2>&1
  echo "$url"
}

seed_engine() {
  local eng=$1                                   # own line (bash 3.2 same-line ${eng} gotcha)
  local name="rivet-oracle-eng-${eng}-${2//./_}" seed; seed="$ROOT/$(cfg seed "$eng")"
  case "$eng" in
    postgres) docker exec -i "$name" psql -U rivet -d rivet -q -v ON_ERROR_STOP=1 < "$seed" 2>&1 | grep -aiE "error" | head -3;;
    mysql)    docker exec -i "$name" mysql -urivet -privet rivet < "$seed" 2>&1 | grep -aiE "error" | head -3;;
    mssql)    docker cp "$seed" "$name:/tmp/s.sql" >/dev/null 2>&1; docker exec "$name" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -C -i /tmp/s.sql 2>&1 | grep -aiE "error|msg [0-9]" | head -3;;
    mongo)    RIVET_MONGO_URI="$3" RIVET_SEED_USERS=150000 RIVET_SEED_ORDERS=150000 python3 "$seed" 2>&1 | grep -aiE "error" | head -3;;
  esac
}

# Scenario implementations live in lib/scenarios.sh (sourced) so this driver stays
# a readable orchestration loop, not a 1000-line monolith.
source "$HERE/lib/scenarios.sh"

# ── main ──
log "Rivet Release Oracle — $(date -u +%FT%TZ)"
echo "  rivet: $RIVET ($($RIVET --version 2>/dev/null | head -1))"
start_stores

# Preflight: the release BUILD/PUBLISH path itself (the stricter tooling the tag runs
# — cargo-chef manifest parse, --locked lock sync, schema regen). MUST run FIRST,
# before any cargo command below reconciles the working-tree lock out from under the
# `cargo metadata --locked` stale-lock check.
verify_release_build_path
# Preflight: the state-DB migrations must be type-parity across SQLite and Postgres
# (bug #1 shipped an int4 keyset_range that broke every Postgres-state run). Runs once,
# source-agnostic, before the engine loop; SKIP when no Postgres STATE url is set.
verify_state_migrations
# Coverage-ledger drift-guards: every docs/*-matrix.yaml stays honest, or NOT RELEASABLE.
verify_coverage_matrices
# Session-pin survival + connection hygiene through a transaction-mode pooler.
verify_pooler_safety
# Prod topology: rivet reading from a read-REPLICA, not the master (drives the
# replica CDC test — mysql-primary :3308 → mysql-replica :3309). SKIP when the
# `replica` compose profile is down.
verify_replica_read
# CDC end-to-end (the change-data-capture surface the batch scenarios never touch):
# per engine anchor → typed changes → capture → store → INDEPENDENT readback +
# validate + state population + SQLite/Postgres parity + at-least-once crash. Runs
# from the RIVET_CDC_<ENGINE>_URL env vars; SKIP (never a silent pass) when unset.
verify_cdc_e2e
# Regression vs the PREVIOUS RELEASE (not a golden, not itself): the new binary reads
# what the prev release WROTE (format-compat) + is no slower/fatter than the DOWNLOADED
# prev-release binary. Needs RIVET_PREV_RELEASE_BIN + RIVET_REGRESSION_SOURCE_URL; SKIP.
verify_release_regression
# The FLAT-RSS guarantee at scale (rivet's headline value prop, the 454M-row no-OOM
# win): peak RSS at 5M rows must stay flat vs 500K rows — a buffering regression that
# scales RSS with the table ships green through every count check otherwise. Needs
# RIVET_REGRESSION_SOURCE_URL; SKIP.
verify_scale_memory

for eng in $(cfg engines); do
  [ -n "$ENGINES_FILTER" ] && ! grep -qw "$eng" <<<"${ENGINES_FILTER//,/ }" && continue
  while read -r tag image port; do
    [ -z "$tag" ] && continue
    log "$eng $tag ($image)"
    url="$(bring_up "$eng" "$tag" "$image" "$port")"
    [ -z "$url" ] && { add "$eng" "$tag" all "-" SKIP "bring-up failed"; continue; }
    # Seed is idempotent (DROP TABLE IF EXISTS …) → retry a transient failure. A
    # fresh container under load (many accumulated containers on colima) can drop
    # the seed connection mid-stream; a bounded retry keeps the gate from crying
    # wolf on a flake rather than a real seed error.
    serr="RETRY"
    for st in 1 2 3; do serr="$(seed_engine "$eng" "$tag" "$url")"; [ -z "$serr" ] && break; sleep 3; done
    if [ -n "$serr" ]; then
      bad "$eng:$tag seed had errors"; add "$eng" "$tag" seed "-" FAIL "seed errors"; continue
    fi
    ok "seeded"
    run_scenarios "$eng" "$tag" "$url"
    # Tear down this version before the next (unless --keep) so peak container count
    # stays at one engine + the three store fakes — accumulating all versions on
    # colima is what starves fresh connects/seeds and flakes the gate.
    [ "$KEEP" = 1 ] || docker rm -f "rivet-oracle-eng-${eng}-${tag//./_}" >/dev/null 2>&1
  done < <(cfg versions "$eng")
done

# ── final: BigQuery golden ──
if [ "$NO_CLOUD" = 0 ]; then
  run_bigquery_golden
fi

# ── report ──
echo; log "Release Oracle result"
printf '  %-10s %-6s %-16s %-8s %s\n' ENGINE VER SCENARIO STORE STATUS
for r in "${RESULTS[@]}"; do IFS='|' read -r e v s st status detail <<<"$r"
  printf '  %-10s %-6s %-16s %-8s %s %s\n' "$e" "$v" "$s" "$st" "$status" "$detail"; done
echo
if [ "$RED" = 0 ]; then printf '\033[1;32m  RELEASE-READY — every non-skipped cell is green.\033[0m\n'; exit 0
else printf '\033[1;31m  NOT RELEASABLE — one or more cells failed (see ✗ above).\033[0m\n'; exit 1; fi
