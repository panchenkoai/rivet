#!/usr/bin/env bash
# The 5.7 scout lane's expected outcome: MySQL CDC refuses a server without
# binlog_row_metadata with RIVET_SOURCE_CDC_PREREQUISITE, before any anchor or part.
# Usage: scout_mysql57_refusal.sh <rivet binary> [<container> [<port>]]
set -euo pipefail
RIVET=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
C=${2:-$(docker compose --profile cdc ps -q mysql-cdc)}
PORT=${3:-3307}
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT
fail() { echo "FAIL: $*"; echo "--- run output:"; cat "$W/run.log" 2>/dev/null || true; exit 1; }

docker exec "$C" mysql -urivet -privet rivet -Ne "SELECT @@version" | grep -q '^5\.7\.' || fail "the server is not MySQL 5.7"
docker exec "$C" mysql -urivet -privet rivet -e \
  "DROP TABLE IF EXISTS scout_t; CREATE TABLE scout_t (id BIGINT PRIMARY KEY, v INT); INSERT INTO scout_t VALUES (1, 1);"
cd "$W"
export RIVET_SCOUT_URL="mysql://rivet:rivet@127.0.0.1:$PORT/rivet"
"$RIVET" init --source-env RIVET_SCOUT_URL --mode cdc --include scout_t -o c.yaml || fail "init failed"
set +e
"$RIVET" run -c c.yaml >run.log 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || fail "the run succeeded on a server rivet must refuse"
[ "$rc" -ne 101 ] || fail "the run panicked (exit 101) instead of refusing"
grep -q '\[RIVET_SOURCE_CDC_PREREQUISITE\]' run.log || fail "no RIVET_SOURCE_CDC_PREREQUISITE code"
grep -q 'no binlog_row_metadata variable' run.log || fail "the refusal does not name the missing variable"
[ -z "$(find . -name '*.ckpt' -o -name '*.parquet')" ] || fail "a refused run left a checkpoint or a part: $(find . -name '*.ckpt' -o -name '*.parquet')"
echo "OK: MySQL 5.7 refused with RIVET_SOURCE_CDC_PREREQUISITE (exit $rc), no anchor, no part"
