#!/usr/bin/env bash
# CDC test stand — one command to stand up the four CDC engines and run the
# scenarios we guard: correctness (the live_cdc suite), capture throughput
# (dev/cdc/perf.sh), and steady-state memory + completeness (soak_all.sh).
#
#   dev/cdc/stand.sh up          # bring the cdc compose profile up + verify ready
#   dev/cdc/stand.sh verify      # re-check each engine is CDC-ready
#   dev/cdc/stand.sh scenarios   # run the live_cdc scenario suite (serial)
#   dev/cdc/stand.sh perf        # capture-throughput matrix (all 4 engines)
#   dev/cdc/stand.sh soak pg     # steady-state memory soak for one engine
#   dev/cdc/stand.sh all         # up + verify + scenarios + perf
#   dev/cdc/stand.sh down        # stop the cdc profile
#
# The stand is the shared `cdc` docker profile (postgres-cdc:5434 / mysql-cdc:3307
# / mssql-cdc:1434 / mongo-rs:27018), isolated from the batch stack's ports. The
# scenario coverage map and the honest thin-spots are in dev/cdc/README.md.
set -uo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)"

CDC_SVCS="postgres-cdc mysql-cdc mssql-cdc mongo-rs"
say() { echo "[cdc-stand] $*"; }
die() { echo "[cdc-stand] ERROR: $*" >&2; exit 1; }

up() {
  say "docker compose --profile cdc up -d ($CDC_SVCS)"
  docker compose --profile cdc up -d $CDC_SVCS || die "compose up failed"
  say "waiting for containers to be healthy..."
  for _ in $(seq 1 60); do
    local n; n=$(docker inspect -f '{{.State.Health.Status}}' rivet-postgres-cdc-1 rivet-mysql-cdc-1 rivet-mssql-cdc-1 rivet-mongo-rs-1 2>/dev/null | grep -c healthy)
    [ "$n" -ge 4 ] && break; sleep 3
  done
  # mssql-cdc has no init hook — the `rivet` DB must be created by hand.
  say "ensuring the mssql-cdc 'rivet' database exists"
  docker exec rivet-mssql-cdc-1 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa \
    -P 'Rivet_Passw0rd!' -Q "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;" 2>/dev/null || true
  verify
}

verify() {
  local ok=1
  local pg my ms mo
  pg=$(docker exec rivet-postgres-cdc-1 psql -U rivet -d rivet -tAc "SHOW wal_level" 2>/dev/null)
  [ "$pg" = logical ] && say "postgres-cdc: wal_level=logical OK" || { say "postgres-cdc: wal_level='$pg' (need logical)"; ok=0; }
  my=$(docker exec rivet-mysql-cdc-1 mysql -urivet -privet -N -e "SELECT @@binlog_format" 2>/dev/null)
  [ "$my" = ROW ] && say "mysql-cdc: binlog_format=ROW OK" || { say "mysql-cdc: binlog_format='$my' (need ROW)"; ok=0; }
  ms=$(docker exec rivet-mssql-cdc-1 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Rivet_Passw0rd!' -h -1 \
        -Q "SET NOCOUNT ON; SELECT CASE WHEN DB_ID('rivet') IS NULL THEN 'no-db' WHEN (SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%')='Running' THEN 'ready' ELSE 'agent-down' END" 2>/dev/null | tr -d '[:space:]')
  [ "$ms" = ready ] && say "mssql-cdc: rivet DB + Agent running OK" || { say "mssql-cdc: '$ms' (need rivet DB + Agent)"; ok=0; }
  mo=$(docker exec rivet-mongo-rs-1 mongosh --quiet --port 27017 --eval "db.isMaster().ismaster" 2>/dev/null | tr -d '[:space:]')
  [ "$mo" = true ] && say "mongo-rs: replica-set primary OK" || { say "mongo-rs: primary='$mo' (need true)"; ok=0; }
  [ "$ok" = 1 ] && say "verify: ALL FOUR ENGINES CDC-READY" || die "verify: one or more engines not CDC-ready"
}

scenarios() {
  say "building + running the live_cdc scenario suite (serial — shared DBs race in parallel)"
  cargo test --test live_suite -- --ignored --test-threads=1 \
    live_cdc:: live_cdc_mssql:: live_cdc_mongo::
}

perf() { RIVET_BIN="${RIVET_BIN:-target/release/rivet}" bash dev/cdc/perf.sh "$@"; }

soak() {
  local eng="${1:?usage: stand.sh soak <pg|mysql|mssql|mongo> [phases]}"; shift || true
  RIVET_BIN="${RIVET_BIN:-target/release/rivet}" PHASES="${*:-10 20 30 60 120}" \
    bash dev/cdc_interval/soak_all.sh "$eng"
}

down() { say "docker compose --profile cdc stop ($CDC_SVCS)"; docker compose --profile cdc stop $CDC_SVCS; }

case "${1:-}" in
  up)        up ;;
  verify)    verify ;;
  scenarios) scenarios ;;
  perf)      shift; perf "$@" ;;
  soak)      shift; soak "$@" ;;
  all)       up && scenarios && perf ;;
  down)      down ;;
  *) echo "usage: dev/cdc/stand.sh <up|verify|scenarios|perf|soak <engine>|all|down>"; exit 1 ;;
esac
