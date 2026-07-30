#!/usr/bin/env bash
# Bring up the version-matrix stand (both tiers unless one is named) and
# finish the seeding the initdb hooks cannot do:
#   - MSSQL (no initdb hook in the image)          → host sqlcmd
#   - Mongo CDC replica sets (no PRIMARY at initdb) → docker exec mongo(sh)
# Idempotent: safe to re-run any time.
#
#   ./up.sh          # batch + cdc
#   ./up.sh batch    # batch tier only
#   ./up.sh cdc      # cdc tier only
set -euo pipefail
cd "$(dirname "$0")"

SA_PASS='Rivet_Passw0rd!'
tier="${1:-all}"
profiles=()
case "$tier" in
  batch) profiles+=(--profile batch) ;;
  cdc)   profiles+=(--profile cdc) ;;
  all)   profiles+=(--profile batch --profile cdc) ;;
  *) echo "usage: $0 [batch|cdc]" >&2; exit 2 ;;
esac

docker compose "${profiles[@]}" up -d

echo "== waiting for health"
for _ in $(seq 1 180); do
  unhealthy=$(docker compose "${profiles[@]}" ps --format '{{.Name}} {{.Health}}' | grep -cEv 'healthy$' || true)
  [ "$unhealthy" = "0" ] && break
  sleep 5
done
docker compose "${profiles[@]}" ps --format 'table {{.Name}}\t{{.Status}}'
if docker compose "${profiles[@]}" ps --format '{{.Health}}' | grep -qv healthy; then
  echo "ERROR: some services never became healthy" >&2
  exit 1
fi

seed_mssql() { # port, seed file
  local port="$1" file="$2"
  if sqlcmd -S "127.0.0.1,$port" -U sa -P "$SA_PASS" -C -d rivet \
       -Q "SET NOCOUNT ON; SELECT TOP 1 1 FROM dbo.users" -h -1 >/dev/null 2>&1; then
    echo "mssql:$port already seeded"
  else
    echo "seeding mssql:$port from $file"
    sqlcmd -S "127.0.0.1,$port" -U sa -P "$SA_PASS" -C -i "$file" >/dev/null
  fi
}

# Mongo seeding: seeds/mongo_seed.py (host python3, stdlib) prints
# Extended-JSON; mongoimport INSIDE the container loads it. No driver on the
# host, no JS files, and the $number* wrappers pin identical BSON types on
# every mongo version.
mongo_eval() { # container, js-expression
  docker exec "$1" mongosh --quiet --eval "$2" 2>/dev/null \
    || docker exec "$1" mongo --quiet --eval "$2"
}

seed_mongo() { # container, orders_count, events_count, user_mod
  local c="$1" orders="$2" events="$3" umod="$4" have
  have=$(mongo_eval "$c" 'print(db.getSiblingDB("rivet").orders.countDocuments({}) + "/" + db.getSiblingDB("rivet").events.countDocuments({}))' | tr -d '[:space:]')
  if [ "$have" = "$orders/$events" ]; then
    echo "$c already seeded ($have)"
    return
  fi
  echo "seeding $c (orders $orders, events $events)"
  python3 seeds/mongo_seed.py orders "$orders" "$umod" \
    | docker exec -i "$c" mongoimport --quiet -d rivet -c orders --drop
  python3 seeds/mongo_seed.py events "$events" "$umod" \
    | docker exec -i "$c" mongoimport --quiet -d rivet -c events --drop
  mongo_eval "$c" 'var d=db.getSiblingDB("rivet"); d.orders.createIndex({user_id:1}); d.events.createIndex({user_id:1}); d.events.createIndex({occurred_at:1});' >/dev/null
}

if [ "$tier" != "cdc" ]; then
  seed_mssql 1519 seeds/common/mssql.sql
  seed_mssql 1522 seeds/common/mssql.sql
  seed_mongo stand-mongo44-batch-1 200000 500000 50000
  seed_mongo stand-mongo50-batch-1 200000 500000 50000
  seed_mongo stand-mongo80-batch-1 200000 500000 50000
fi
if [ "$tier" != "batch" ]; then
  seed_mssql 1619 seeds/cdc/mssql.sql
  seed_mssql 1622 seeds/cdc/mssql.sql
  seed_mongo stand-mongo44-cdc-1 10 10 10
  seed_mongo stand-mongo50-cdc-1 10 10 10
  seed_mongo stand-mongo80-cdc-1 10 10 10
fi

echo "== done; run ./verify.sh to cross-check the seeds"
