#!/usr/bin/env bash
# Governor self-throttle A/B stand.
#
# Runs the same self-spilling MySQL export through two rivet binaries × two
# tuning modes (adaptive on/off) and grades each binary on the class invariant
# behind the 2026-08-13 field regression: ON AN IDLE SOURCE, adaptivity has
# nothing legitimate to react to, so `adaptive: true` must shed nothing and
# cost ~nothing vs `adaptive: false`. A binary whose governor listens to a
# counter the export itself inflates (the 0.24.4 bug) sheds to min_parallel
# and fails both gates.
#
# Fixture: DISTINCT over a wide payload — every chunk materializes the whole
# derived table; with tmp_table_size forced to 16 KB (root-flipped globals,
# restored on exit) every chunk provably bumps Created_tmp_disk_tables, the
# exact counter the buggy governor listened to.
#
# Usage:
#   RIVET_A=~/.local/bin/rivet RIVET_B=target/release/rivet dev/governor-stand/ab.sh
# Env (defaults match the dev docker-compose):
#   MYSQL_URL       mysql://rivet:rivet@127.0.0.1:3306/rivet
#   MYSQL_ROOT_URL  root connection for the tmp-table globals flip
set -euo pipefail

RIVET_A=${RIVET_A:?path to baseline rivet binary}
RIVET_B=${RIVET_B:?path to candidate rivet binary}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_CONT=${MYSQL_CONT:-rivet-mysql-1}
ROWS=${ROWS:-60000}
TBL=gov_stand_ab

mysql_root() { docker exec -i "$MYSQL_CONT" mysql -uroot -privet rivet -N -e "$1" 2>/dev/null; }

echo "== seeding ${TBL} (${ROWS} rows, wide payload)"
mysql_root "DROP TABLE IF EXISTS ${TBL}; CREATE TABLE ${TBL} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB;"
for s in $(seq 1 5000 $((ROWS - 4999))); do
  mysql_root "SET SESSION cte_max_recursion_depth=6000; INSERT INTO ${TBL} SELECT seq, REPEAT('x',1024) FROM (WITH RECURSIVE s(seq) AS (SELECT $s UNION ALL SELECT seq+1 FROM s WHERE seq < $s+4999) SELECT seq FROM s) t;"
done

echo "== forcing tmp-table spills (globals flipped; restored on exit)"
PRIOR_ENGINE=$(mysql_root "SELECT @@internal_tmp_mem_storage_engine;")
PRIOR_TMP=$(mysql_root "SELECT @@tmp_table_size;")
PRIOR_HEAP=$(mysql_root "SELECT @@max_heap_table_size;")
restore() {
  mysql_root "SET GLOBAL internal_tmp_mem_storage_engine=${PRIOR_ENGINE}; SET GLOBAL tmp_table_size=${PRIOR_TMP}; SET GLOBAL max_heap_table_size=${PRIOR_HEAP}; DROP TABLE IF EXISTS ${TBL};" || true
}
trap restore EXIT
mysql_root "SET GLOBAL internal_tmp_mem_storage_engine=MEMORY; SET GLOBAL tmp_table_size=16384; SET GLOBAL max_heap_table_size=16384;"

WORK=$(mktemp -d)
run_one() { # $1 binary  $2 adaptive  $3 tag
  local bin=$1 adaptive=$2 tag=$3
  local dir="$WORK/$tag"
  mkdir -p "$dir"
  cat > "$dir/cfg.yaml" <<EOF
source:
  type: mysql
  url: "mysql://rivet:rivet@${MYSQL_HOST}:3306/rivet"
  tuning:
    adaptive: ${adaptive}
    min_parallel: 1
    batch_size: 250
exports:
  - name: ${TBL}
    query: "SELECT DISTINCT id, payload FROM ${TBL}"
    mode: chunked
    chunk_column: id
    chunk_size: 3000
    parallel: 4
    format: parquet
    destination: { type: local, path: ${dir}/out }
EOF
  local t0=$(python3 -c 'import time; print(time.time())')
  RUST_LOG=info RIVET_GOVERNOR_INTERVAL_MS=200 "$bin" run -c "$dir/cfg.yaml" >/dev/null 2> "$dir/stderr.log"
  local t1=$(python3 -c 'import time; print(time.time())')
  local wall; wall=$(python3 -c "print(f'{$t1-$t0:.2f}')")
  local sheds; sheds=$(grep -c "backed off" "$dir/stderr.log" || true)
  echo "$wall $sheds"
}

verdict_for() { # $1 binary-path  $2 label
  local bin=$1 label=$2
  read -r wall_off _ <<< "$(run_one "$bin" false "${label}_off")"
  read -r wall_on sheds <<< "$(run_one "$bin" true "${label}_on")"
  local ratio; ratio=$(python3 -c "print(f'{$wall_on/$wall_off:.2f}')")
  local pass="PASS"
  python3 -c "exit(0 if $sheds==0 and $wall_on <= $wall_off*1.6+1.0 else 1)" || pass="FAIL (self-throttle)"
  printf "%-10s off=%ss on=%ss ratio=%s sheds=%s → %s\n" "$label" "$wall_off" "$wall_on" "$ratio" "$sheds" "$pass"
}

echo "== A: $($RIVET_A --version 2>/dev/null || echo unknown) ($RIVET_A)"
echo "== B: $($RIVET_B --version 2>/dev/null || echo unknown) ($RIVET_B)"
echo
verdict_for "$RIVET_A" "A"
verdict_for "$RIVET_B" "B"
