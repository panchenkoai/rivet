# ── Release-oracle scenarios (sourced by run.sh) ────────────────────────────
# Each scenario is a function taking (engine, tag, url). It records PASS/FAIL/SKIP
# via add() + ok()/bad()/skip() from run.sh. Reuses the canonical seeds (already
# applied) and the DuckDB independent oracle — the same discipline the manual
# dogfood used, now deterministic and repeatable.

# ── keyset_parallel: the feat/parallel-keyset fan-out, end-to-end per engine ──
# The sequential keyset path is already exercised by integrity_types/load. This
# scenario proves the PARALLEL variant on the same 150K users table: (1) the same
# independent DuckDB loss/dup oracle (no row lost or duped across the N ranges),
# AND (2) the run actually FANNED OUT — parallel:4 must produce >=2 distinct
# `_pk_w{ridx}_` worker parts, else it silently degraded to sequential and the
# headline feature never engaged. Mongo's `parallel:N` is the separate _id-range
# reader (mongo_parallel), not SQL keyset, so it is NA here.
sc_keyset_parallel() {
  local eng=$1 tag=$2 url=$3
  [ "$eng" = mongo ] && { skip "keyset_parallel[mongo]: separate _id-range path (mongo_parallel), not SQL keyset"; add "$eng" "$tag" keyset_parallel - SKIP "mongo na"; return; }
  local out="$WORK/kp_${eng}_${tag//./_}"   # own line — bash 3.2 same-line ${eng} gotcha
  mkdir -p "$out"
  if ! _export_local "$eng" "$url" users "$out/users" chunked parquet 4; then
    bad "keyset_parallel[$eng]: export failed"; add "$eng" "$tag" keyset_parallel - FAIL "export"; return
  fi
  local fails=""
  # (1) loss/dup — the SAME independent oracle integrity_types uses.
  local scnt dcnt
  scnt="$(_source_count_distinct "$eng" "$url" users id)"
  dcnt="$(duckdb -noheader -list -c "SELECT count(*)||' '||count(DISTINCT id) FROM read_parquet('$out/users/**/*.parquet')" 2>/dev/null)"
  [ "$scnt" = "$dcnt" ] || fails+="loss/dup src[$scnt]!=parquet[$dcnt] "
  # (2) fan-out — >=2 distinct pk_w{ridx} workers (find, not ** — bash 3.2 has no globstar).
  local workers
  workers="$(find "$out/users" -name '*.parquet' 2>/dev/null | grep -oE '_pk_w[0-9]+_' | sort -u | wc -l | tr -d ' ')"
  [ "${workers:-0}" -ge 2 ] || fails+="fan-out workers=${workers:-0} (parallel:4 degraded to sequential) "
  [ -z "$fails" ] && { ok "keyset_parallel (loss/dup 0, fan-out $workers workers)"; add "$eng" "$tag" keyset_parallel - PASS "$workers workers"; } \
                  || { bad "keyset_parallel: $fails"; add "$eng" "$tag" keyset_parallel - FAIL "$fails"; }
}

# ── state-migration parity PREFLIGHT (source-agnostic, runs once) ────────────
# Bug #1 (int4 keyset_range) proved the state layer has TWO migration arrays
# (MIGRATIONS/SQLite, PG_MIGRATIONS/Postgres) behind a dialect seam that ASSUMES they
# are type-compatible — and nothing verified it, so a Postgres-only schema drift was
# invisible to every SQLite unit test. This preflight INITs (migrates) + RUNs the
# state-exercising golden fixtures on BOTH backends and compares: the RED-proven
# tests/live/live_state_backend_parity.rs (+ the keyset_range round-trip), driven
# through the RELEASE binary so the gate blesses what ships. A schema drift makes the
# Postgres run abort → the parity assert fails → the release is NOT releasable.
# Needs a Postgres STATE db (RIVET_TEST_STATE_URL) + the source Postgres on :5432 +
# cargo; SKIP (never a silent pass) when any is absent.
verify_state_migrations() {
  local st="${RIVET_TEST_STATE_URL:-}"
  [ -z "$st" ] && { skip "state-migration parity: no Postgres STATE url (set RIVET_TEST_STATE_URL)"; add state migrations parity - SKIP "no state url"; return; }
  # The url MUST be postgres — the underlying tests self-skip (return) on any other
  # scheme, which libtest counts as PASSED. Without this, RIVET_TEST_STATE_URL=sqlite://…
  # would print a green "SQLite == Postgres" having verified NOTHING (a false-pass gate).
  case "$st" in postgres*|postgresql*) ;; *) skip "state-migration parity: RIVET_TEST_STATE_URL is not a postgres url"; add state migrations parity - SKIP "non-postgres url"; return;; esac
  command -v cargo >/dev/null 2>&1 || { skip "state-migration parity: cargo absent"; add state migrations parity - SKIP "no cargo"; return; }
  # Source Postgres must be reachable (the parity fixtures seed a source table on :5432).
  if ! (exec 3<>/dev/tcp/127.0.0.1/5432) 2>/dev/null; then
    skip "state-migration parity: source Postgres :5432 down"; add state migrations parity - SKIP "no source pg"; return
  fi
  exec 3>&- 2>/dev/null || true
  log "State-migration parity (SQLite vs Postgres state, RED-proven, release binary)"
  # Drive the parity + round-trip live tests against the RELEASE binary + real PG state.
  # The test binary itself is debug (fast to build); it only orchestrates — the rivet
  # process it spawns is the RELEASE binary via RIVET_BIN, so the gate blesses what ships.
  # Parity fixtures on a FRESH db (live_suite) + the in-place UPGRADE path (a lib test
  # that stages a populated v18 db and migrates it to HEAD — a migration that works
  # clean but breaks on populated old data slips past the fresh-db parity otherwise).
  if RIVET_BIN="$RIVET" RIVET_TEST_STATE_URL="$st" \
     cargo test --manifest-path "$ROOT/Cargo.toml" --test live_suite -- --ignored --test-threads=1 \
       state_parity_ pg_keyset_range_round_trips_and_commits >"$WORK/state_parity.log" 2>&1 \
     && RIVET_TEST_STATE_URL="$st" \
        cargo test --manifest-path "$ROOT/Cargo.toml" --lib -- \
          pg_upgrade_from_v18 pg_shared_state_cross_connection >>"$WORK/state_parity.log" 2>&1; then
    ok "state-migration parity: SQLite == Postgres (fresh + upgrade + shared-state concurrency)"
    add state migrations parity - PASS
  else
    bad "state-migration parity FAILED — a schema drift between MIGRATIONS and PG_MIGRATIONS (see $WORK/state_parity.log)"
    add state migrations parity - FAIL "$(grep -aiE 'must succeed|cannot convert|FAILED' "$WORK/state_parity.log" | head -1)"
  fi
}

# ── coverage-ledger drift-guards PREFLIGHT (offline, runs ONCE) ──────────────
# "Are all the coverage matrices in the gate?" — YES: this runs every
# docs/*-matrix.yaml drift-guard (chunking/behaviour/type-fidelity/... via
# chunking_matrix_guard, plus release_gate_matrix_guard + perf_matrix_guard) so the
# go/no-go gate itself blocks on a rotted/drifted ledger, not just CI. A drifted
# matrix means the coverage claims a release rests on are stale -> NOT RELEASABLE.
# SKIP when cargo is absent.
verify_coverage_matrices() {
  command -v cargo >/dev/null 2>&1 || { skip "coverage matrices: cargo absent"; add coverage matrices ledger - SKIP "no cargo"; return; }
  log "Coverage-ledger drift-guards (all docs/*-matrix.yaml)"
  if cargo test --manifest-path "$ROOT/Cargo.toml" --test offline_suite matrix_guard >"$WORK/matrices.log" 2>&1; then
    ok "coverage matrices: all ledgers drift-free"
    add coverage matrices ledger - PASS
  else
    bad "coverage matrices: a ledger drifted (see $WORK/matrices.log)"
    add coverage matrices ledger - FAIL "$(grep -aiE 'FAILED|panicked|assertion' "$WORK/matrices.log" | head -1)"
  fi
}

# ── pooler safety PREFLIGHT (session-pin survival through a transaction pooler) ─
# Prod often runs behind a transaction-mode pooler that hands a DIFFERENT physical
# connection per statement — where a session pin (SET LOCAL / time_zone / sql_mode /
# max_execution_time — the exact leg bug #2 lives on) can leak or vanish. The gate
# otherwise connects DIRECT to every engine, never through a pooler. This drives the
# RED-proven tests/live/live_pool_safety.rs through pgbouncer (pool_size=1 transaction
# mode) + proxysql: the pins reset at COMMIT and the connection is clean after a failed
# export. Needs the `pool` compose profile (pgbouncer :6432 / proxysql :6033); runs
# whichever pooler is up, SKIP when neither is.
verify_pooler_safety() {
  command -v cargo >/dev/null 2>&1 || { skip "pooler safety: cargo absent"; add pooler safety - - SKIP "no cargo"; return; }
  local pgb=0 pxy=0 filters=""
  (exec 3<>/dev/tcp/127.0.0.1/6432) 2>/dev/null && { pgb=1; exec 3>&- 2>/dev/null || true; }
  (exec 3<>/dev/tcp/127.0.0.1/6033) 2>/dev/null && { pxy=1; exec 3>&- 2>/dev/null || true; }
  [ $pgb = 1 ] && filters="$filters pg_statement_timeout_not_leaked_after_successful_export pg_connection_usable_and_clean_after_failed_export"
  [ $pxy = 1 ] && filters="$filters mysql_proxysql_session_vars_clean_after_successful_export mysql_proxysql_session_vars_clean_after_failed_export mysql_proxysql_connection_classified_as_proxysql"
  if [ -z "$filters" ]; then
    skip "pooler safety: no pgbouncer :6432 / proxysql :6033 (docker compose --profile pool up -d)"; add pooler safety - - SKIP "no poolers"; return
  fi
  log "Pooler safety (session-pin survival + connection hygiene through pgbouncer/proxysql)"
  # shellcheck disable=SC2086
  if RIVET_BIN="$RIVET" cargo test --manifest-path "$ROOT/Cargo.toml" --test live_suite -- --ignored --test-threads=1 $filters >"$WORK/pooler.log" 2>&1; then
    ok "pooler safety: session pins reset + connections clean through the pooler ($([ $pgb = 1 ] && printf pgbouncer) $([ $pxy = 1 ] && printf proxysql))"
    add pooler safety - - PASS
  else
    bad "pooler safety FAILED — a session pin leaked or a connection was left dirty through the pooler (see $WORK/pooler.log)"
    add pooler safety - - FAIL "$(grep -aiE 'FAILED|leaked|assert' "$WORK/pooler.log" | head -1)"
  fi
}

run_scenarios() {
  local eng=$1 tag=$2 url=$3
  sc_verdicts "$eng" "$tag" "$url"
  sc_integrity_types "$eng" "$tag" "$url"
  # blessing the local goldens (verdicts + duckdb-type) — skip the store loads.
  { [ "${BLESS_VERDICTS:-0}" = 1 ] || [ "${BLESS_DUCKDB:-0}" = 1 ]; } && return
  sc_keyset_parallel "$eng" "$tag" "$url"
  for store in $(cfg stores); do
    sc_load "$eng" "$tag" "$url" "$store"
  done
  [ "$eng" = postgres ] && sc_gc_survival "$eng" "$tag" "$url"
}

# helper: run rivet init into a config for the whole DB (or --schema for pg/mssql
# garbage), echoing the config path.
_init_cfg() {
  local eng=$1 url=$2 schema=$3
  # NOTE: `out` MUST be its own `local` — on macOS bash 3.2 a same-line
  # `local schema=$3 out="…${schema}…"` expands ${schema} against the ENCLOSING
  # scope (empty), so seed(public) and garbage(ext) both collapsed to one path,
  # the ext init clobbered the public one, and the verdict map lost all seed
  # tables (kept only garbage). Split so ${eng}/${schema} are the just-assigned args.
  local out="$WORK/${eng}_${schema:-all}.yaml"
  export ORACLE_URL="$url"
  # `rivet init` has NO tls flag — it connects to SQL Server with cert-validation
  # disabled by default (emitting a warn), so no flag is needed OR accepted here;
  # passing one makes init abort with a usage error → an empty (0-table) verdict map.
  local schemaflag=""; [ -n "$schema" ] && schemaflag="--schema $schema"
  # shellcheck disable=SC2086
  "$RIVET" init --source-env ORACLE_URL $schemaflag -o "$out" >/dev/null 2>&1 || return 1
  echo "$out"
}

# strategy of one export from a generated config: prints "keyset" | "range" | "full".
_strategy_of() {
  local yaml=$1 name=$2
  local blk; blk="$(awk -v n="  - name: $name" '$0==n{f=1;next} /^  - name:/{f=0} f' "$yaml")"
  if grep -q "chunk_by_key:" <<<"$blk"; then echo keyset
  elif grep -q "chunk_column:" <<<"$blk"; then echo range
  else echo full; fi
}

# ── verdicts: init+check → {table:{strategy,verdict}} == GOLDEN ──────────────
# The golden (golden/verdicts.json) fixes the strategy+verdict of EVERY seed and
# garbage table per engine; a divergence (keyset→full regression, decimal stops
# bailing, name-trap starts keyset'ing) fails the release against a checked-in
# truth. Regenerate with --bless-verdicts-golden (intentional).
GOLDEN_VERDICTS="$HERE/golden/verdicts.json"
sc_verdicts() {
  local eng=$1 tag=$2 url=$3 map="{}" phantom=0
  # `rivet check` reads the source URL from $ORACLE_URL (the init'd configs carry
  # `url_env: ORACLE_URL`). _init_cfg exports it too — but that runs in a $()
  # SUBSHELL, so the export never reaches THIS parent shell where the check loop
  # runs. Without this line the check connected to a STALE url (a prior engine's
  # leftover from `_export_local`) or nothing at all, yielding a 0-table map for
  # the first version of every engine. Export in the parent, once, authoritatively.
  export ORACLE_URL="$url"
  # Build the live map by init+check of every config the engine needs (seeds, +garbage).
  local checks=()
  if [ "$eng" = mongo ]; then
    local c; c="$(_init_cfg mongo "$url" "")" || { skip "mongo init failed"; add mongo "$tag" verdicts - SKIP init; return; }
    checks=("$c")
  else
    # seeds schema is per-engine: PG `public`, MSSQL `dbo`, MySQL none (db from URL).
    # (init WITHOUT --schema on PG scaffolds nothing usable → an empty check.)
    local sch=""; case "$eng" in postgres) sch=public;; mssql) sch=dbo;; esac
    local sc; sc="$(_init_cfg "$eng" "$url" "$sch")" \
      || { skip "$eng init failed"; add "$eng" "$tag" verdicts - SKIP init; return; }
    checks=("$sc")
    # pg/mssql: garbage lives in schema `ext` (separate init); mysql: same DB.
    [ "$eng" != mysql ] && { local gc; gc="$(_init_cfg "$eng" "$url" ext)" && checks+=("$gc"); }
  fi
  for cfg in "${checks[@]}"; do
    local out; out="$("$RIVET" check -c "$cfg" 2>&1)"   # check writes to stderr — capture both
    phantom=$((phantom + $(grep -ac "Heavy chunk" <<<"$out")))
    map="$(python3 "$HERE/lib/parse_verdicts.py" "$map" <<<"$out")"
  done

  if [ "${BLESS_VERDICTS:-0}" = 1 ]; then
    # Never bless an EMPTY map — a failed connect must fail LOUD, not silently write
    # a 0-table golden that asserts nothing on the next release.
    if [ "$map" = "{}" ] || [ -z "$map" ]; then
      bad "verdicts[$eng]: empty map — refusing to bless (check could not connect?)"; add "$eng" "$tag" verdicts - FAIL "empty map"; return; fi
    mkdir -p "$HERE/golden"
    python3 -c "import json,os,sys; g=json.load(open('$GOLDEN_VERDICTS')) if os.path.exists('$GOLDEN_VERDICTS') else {}; g['$eng']=json.loads(sys.argv[1]); open('$GOLDEN_VERDICTS','w').write(json.dumps(g,indent=2,sort_keys=True)+chr(10))" "$map"
    ok "blessed verdicts[$eng]"; add "$eng" "$tag" verdicts - PASS blessed; return
  fi
  # zero phantom heavy-chunk (a cross-check the strategy/verdict golden can't hold).
  [ "$phantom" = 0 ] || { bad "verdicts: $phantom phantom heavy-chunk warning(s)"; add "$eng" "$tag" verdicts - FAIL "phantom-heavy-chunk=$phantom"; return; }
  local want got
  want="$(python3 -c "import json;print(json.dumps(json.load(open('$GOLDEN_VERDICTS')).get('$eng',{}),sort_keys=True))" 2>/dev/null)"
  got="$(python3 -c "import json,sys;print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$map")"
  if [ -z "$want" ] || [ "$want" = "{}" ]; then skip "no golden for $eng (bless first)"; add "$eng" "$tag" verdicts - SKIP "no golden"; return; fi
  if [ "$want" = "$got" ]; then ok "verdicts match golden (0 phantom)"; add "$eng" "$tag" verdicts - PASS
  else local d; d="$(python3 "$HERE/lib/verdict_diff.py" "$want" "$got")"; bad "verdicts DIVERGED: $d"; add "$eng" "$tag" verdicts - FAIL "$d"; fi
}

# ── integrity + types + fidelity ─────────────────────────────────────────────
# Two independent oracles:
#  (1) users 150K loss/dup — source count+distinct vs the DuckDB read of the parts.
#  (2) TYPE + FIDELITY matrices — a DuckDB GOLDEN (golden/duckdb_type_matrix.json,
#      per engine), with TWO arguments per matrix that DuckDB (not rivet) reads back:
#        <tmt>       — PARQUET readback: the binary/typed path (decimal precision,
#                      uuid nulling, timestamp shift, enum).
#        <tmt>__csv  — CSV readback, ALL-VARCHAR: the exact TEXT the writer emitted
#                      (escape/quote/unicode/null fidelity) — the text-writer class
#                      the binary parquet path never exercises.
#      Both compared to the blessed golden — a regression fails against a fixed truth,
#      exactly like the BigQuery golden but on the local independent reader.
GOLDEN_DUCKDB="$HERE/golden/duckdb_type_matrix.json"
sc_integrity_types() {
  local eng=$1 tag=$2 url=$3 fails=""
  local out="$WORK/it_${eng}_${tag//./_}"   # own line — bash 3.2 same-line ${eng} gotcha
  mkdir -p "$out"
  # (1) users loss/dup — all engines.
  if _export_local "$eng" "$url" users "$out/users" chunked; then
    local scnt dcnt
    scnt="$(_source_count_distinct "$eng" "$url" users id)"
    dcnt="$(duckdb -noheader -list -c "SELECT count(*)||' '||count(DISTINCT $( [ "$eng" = mongo ] && echo _id || echo id )) FROM read_parquet('$out/users/**/*.parquet')" 2>/dev/null)"
    [ "$scnt" = "$dcnt" ] || fails+="users src[$scnt]!=parquet[$dcnt] "
  else
    fails+="users-export-failed "
  fi
  # (2) type + fidelity matrices → DuckDB golden (SQL engines have rivet_type_matrix
  #     [_full]). TWO golden arguments per matrix, each an independent fidelity check
  #     via DuckDB's own reader (never rivet's):
  #       <tmt>       — PARQUET readback: the binary/typed path (decimal precision,
  #                     uuid, timestamp, enum).
  #       <tmt>__csv  — CSV readback, read ALL-VARCHAR so the compare is on the exact
  #                     TEXT rivet's writer emitted (escape/quote/unicode/null
  #                     fidelity) — the text-writer class the binary parquet path
  #                     never exercises. Regressions fail against a fixed truth.
  if [ "$eng" != mongo ]; then
    # ONE comprehensive matrix per engine now (the canonical full type set —
    # `rivet_type_matrix` ports tests/type_roundtrip/fixtures/<eng>_schema.sql).
    for tmt in rivet_type_matrix; do
      if _export_local "$eng" "$url" "$tmt" "$out/$tmt" full; then
        local got; got="$(duckdb -json -c "SELECT * FROM read_parquet('$out/$tmt/**/*.parquet') ORDER BY id" 2>/dev/null | python3 "$HERE/lib/normalize_bq.py")"
        if [ -z "$got" ]; then fails+="$tmt-readback "; else _fidelity_check "$eng" "$tmt" "$got" || fails+="$tmt-TYPE-DIVERGED "; fi
      else fails+="$tmt-export "; fi
      # CSV fidelity. rivet REFUSES a nested/array column in CSV (PG text[] → "CSV
      # cannot serialize"); that refusal is itself a fidelity guarantee (no silent
      # lossy array→CSV), so record the serialized text when it serializes and a
      # refusal SENTINEL when it doesn't — either way a fixed truth the compare
      # asserts (a regression that starts emitting a corrupted array flips it).
      if _export_local "$eng" "$url" "$tmt" "$out/${tmt}_csv" full csv \
         && [ -n "$(ls "$out/${tmt}_csv"/**/*.csv "$out/${tmt}_csv"/*.csv 2>/dev/null)" ]; then
        local gotc; gotc="$(duckdb -json -c "SELECT * FROM read_csv('$out/${tmt}_csv/**/*.csv', all_varchar=true, header=true) ORDER BY id" 2>/dev/null | python3 "$HERE/lib/normalize_bq.py")"
        if [ -z "$gotc" ]; then fails+="${tmt}-csv-readback "; else _fidelity_check "$eng" "${tmt}__csv" "$gotc" || fails+="${tmt}-CSV-DIVERGED "; fi
      else
        _fidelity_check "$eng" "${tmt}__csv" '{"csv_writer":"refused-unrepresentable-column"}' || fails+="${tmt}-CSV-REFUSAL-CHANGED "
      fi
    done
  fi
  if [ "${BLESS_DUCKDB:-0}" = 1 ]; then ok "blessed duckdb-types[$eng]"; add "$eng" "$tag" integrity_types - PASS blessed; return; fi
  [ -z "$fails" ] && { ok "integrity+types (loss/dup 0, type matrices match DuckDB golden)"; add "$eng" "$tag" integrity_types - PASS; } \
                  || { bad "integrity+types: $fails"; add "$eng" "$tag" integrity_types - FAIL "$fails"; }
}

# ── golden helpers (shared by duckdb + bigquery goldens) ─────────────────────
_json_canon() { python3 -c "import json,sys;print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$1" 2>/dev/null; }
_golden_get() { python3 -c "import json,os,sys;g=json.load(open(sys.argv[1])) if os.path.exists(sys.argv[1]) else {};print(json.dumps(g.get(sys.argv[2],{}).get(sys.argv[3]),sort_keys=True) if g.get(sys.argv[2],{}).get(sys.argv[3]) is not None else '')" "$1" "$2" "$3" 2>/dev/null; }
_golden_put() { mkdir -p "$(dirname "$1")"; python3 -c "import json,os,sys;g=json.load(open(sys.argv[1])) if os.path.exists(sys.argv[1]) else {};g.setdefault(sys.argv[2],{})[sys.argv[3]]=json.loads(sys.argv[4]);open(sys.argv[1],'w').write(json.dumps(g,indent=2,sort_keys=True)+chr(10))" "$1" "$2" "$3" "$4"; }

# bless-or-compare ONE golden argument (engine, key, got-json) vs the DuckDB golden.
# bless mode → record & succeed; compare mode → 0 iff it equals the blessed value
# (a missing golden is a failure — bless first).
_fidelity_check() {
  local eng=$1 key=$2 got=$3
  if [ "${BLESS_DUCKDB:-0}" = 1 ]; then _golden_put "$GOLDEN_DUCKDB" "$eng" "$key" "$got"; return 0; fi
  local want; want="$(_golden_get "$GOLDEN_DUCKDB" "$eng" "$key")"
  [ -n "$want" ] && [ "$(_json_canon "$got")" = "$want" ]
}

_source_count_distinct() {
  local eng=$1 url=$2 tbl=$3 idc=$4 name="rivet-oracle-eng-${eng}-*"
  local c; c="$(docker ps --format '{{.Names}}' | grep "rivet-oracle-eng-${eng}-" | head -1)"
  case "$eng" in
    postgres) docker exec "$c" psql -U rivet -d rivet -tA -c "SELECT count(*)||' '||count(DISTINCT $idc) FROM $tbl" 2>/dev/null;;
    mysql)    docker exec "$c" mysql -urivet -privet rivet -N -e "SELECT CONCAT(count(*),' ',count(DISTINCT $idc)) FROM $tbl" 2>/dev/null;;
    mssql)    docker exec "$c" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -C -h -1 -W -Q "SET NOCOUNT ON; SELECT CONCAT(count(*),' ',count(DISTINCT $idc)) FROM $tbl" 2>/dev/null | tr -d '\r';;
    mongo)    # mongo:4.4 ships the legacy `mongo` shell; 5.0+ ships `mongosh`. Pick
              # whichever the container actually has, else the count returns an OCI
              # "executable not found" string and the gate false-fails on good data.
              local msh=mongosh; docker exec "$c" sh -c 'command -v mongosh >/dev/null 2>&1' || msh=mongo
              # countDocuments({}) NOT countDocuments() — the legacy 4.4 shell rejects the
              # no-arg form ("match filter must be an expression in an object").
              docker exec "$c" "$msh" mongodb://127.0.0.1:27017/rivet --quiet --eval "print(db.$tbl.countDocuments({})+' '+db.$tbl.distinct('_id').length)" 2>/dev/null;;
  esac
}

_type_oracle() {  # pg/mysql: decimal sum + uuid distinct match source (scanner)
  local eng=$1 tbl=$2 dir=$3 port=$4 att
  case "$eng" in
    postgres) att="INSTALL postgres_scanner; LOAD postgres_scanner; ATTACH 'postgresql://rivet:rivet@127.0.0.1:$port/rivet' AS s (TYPE postgres, READ_ONLY);";;
    mysql)    att="INSTALL mysql_scanner; LOAD mysql_scanner; ATTACH 'host=127.0.0.1 port=$port user=rivet password=rivet database=rivet' AS s (TYPE mysql, READ_ONLY);";;
  esac
  local r; r="$(duckdb -noheader -list -c "$att
    CREATE TABLE d AS SELECT * FROM read_parquet('$dir/**/*.parquet');
    SELECT CASE WHEN (SELECT sum(amount) FROM s.$tbl)=(SELECT sum(amount) FROM d)
                 AND (SELECT count(DISTINCT uid::text) FROM s.$tbl)=(SELECT count(DISTINCT uid::text) FROM d)
            THEN 'ok' ELSE 'MISMATCH' END" 2>/dev/null)"
  echo "${r:-oracle-error}"
}

_hostport() { docker ps --format '{{.Names}} {{.Ports}}' | grep "rivet-oracle-eng-$1-" | grep -oE ':[0-9]+->(5432|3306|1433|27017)' | head -1 | grep -oE '[0-9]+' | head -1; }

# export one table to a LOCAL dir. mode: chunked|full. fmt: parquet|csv (default parquet).
_export_local() {
  local eng=$1 url=$2 tbl=$3 dir=$4 mode=$5 fmt=${6:-parquet} parallel=${7:-}
  local yaml="$WORK/ex_$$_${tbl//./_}_${fmt}.yaml"   # own line — bash 3.2 same-line ${tbl} gotcha
  rm -rf "$dir"; export ORACLE_URL="$url"
  [ "$eng" = mongo ] && mode=full   # Mongo has no keyset/chunked — full scan only
  local keyline="    mode: full"
  if [ "$mode" = chunked ]; then
    keyline=$'    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000'
    # parallel: N fans keyset into N ROW-percentile ranges (feat/parallel-keyset).
    [ -n "$parallel" ] && keyline+=$'\n    parallel: '"$parallel"
  fi
  local tlsblk=""; [ "$eng" = mssql ] && tlsblk=$'\n  tls: {accept_invalid_certs: true}'
  cat > "$yaml" <<YAML
source:
  type: $eng
  url_env: ORACLE_URL$tlsblk
exports:
  - name: $tbl
    table: $tbl
$keyline
    format: $fmt
    destination: {type: local, path: $dir/}
YAML
  "$RIVET" run -c "$yaml" >/dev/null 2>&1
}

# ── load: export TO the object store WITH --validate (rivet re-reads the parts
# back FROM the store and checks Form-B integrity — a real write→store→read
# round-trip, store-agnostic, no external downloader) ────────────────────────
sc_load() {
  local eng=$1 tag=$2 url=$3 store=$4
  _store_up "$store" || { skip "$store emulator down"; add "$eng" "$tag" load "$store" SKIP "emulator down"; return; }
  # Run-UNIQUE prefix (${WORK##*/} is this run's mktemp token): run-unique PART
  # names never clobber prior runs (by design), so a stable prefix makes the
  # readback count EVERY past run's parts (N×150k). A fresh prefix per run isolates
  # this run's load so the count gate compares 150k-to-150k, not an accumulation.
  local bkt; bkt="$(cfg store "$store" bucket)"; local pfx="oracle/${WORK##*/}/${eng}_${tag//./_}/${store}"
  local dest; dest="$(_store_dest "$store" "$bkt" "$pfx")" || { skip "$store: no dest config"; add "$eng" "$tag" load "$store" SKIP "no dest"; return; }
  local yaml="$WORK/load_${eng}_${tag//./_}_${store}.yaml"; export ORACLE_URL="$url"
  export MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin AZURITE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  local tlsblk=""; [ "$eng" = mssql ] && tlsblk=$'\n  tls: {accept_invalid_certs: true}'
  local modeblk=$'    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000'
  [ "$eng" = mongo ] && modeblk="    mode: full"   # Mongo: full scan only
  cat > "$yaml" <<YAML
source:
  type: $eng
  url_env: ORACLE_URL$tlsblk
exports:
  - name: users
    table: users
$modeblk
    format: parquet
    destination:
$dest
YAML
  # rivet run extracts to the store; the readback is INDEPENDENT (the store's own
  # tooling + DuckDB — gcloud/gsutil for GCS, s3 for MinIO, az for Azure — NOT
  # rivet's own --validate, so a rivet read bug can't rubber-stamp its own write).
  local out; out="$("$RIVET" run -c "$yaml" 2>&1)"
  if grep -qaiE "error|failed" <<<"$out"; then
    bad "load→$store export failed"; add "$eng" "$tag" load "$store" FAIL "$(grep -aiE 'error|fail' <<<"$out" | head -1)"; return; fi
  local n; n="$(_store_readback "$store" "$bkt" "$pfx")"
  local scnt; scnt="$(_source_count_distinct "$eng" "$url" users id | cut -d' ' -f1)"
  if [ -n "$n" ] && [ "$n" = "$scnt" ]; then ok "load→$store gcloud-verified $n rows"; add "$eng" "$tag" load "$store" PASS "$n"
  elif [ -z "$n" ]; then skip "$store readback tool unavailable"; add "$eng" "$tag" load "$store" SKIP "no readback tool"
  else bad "load->${store} readback ${n} != source ${scnt}"; add "$eng" "$tag" load "$store" FAIL "${n}!=${scnt}"; fi
}

# INDEPENDENT readback: count the parts the store actually holds, via the store's
# OWN client + DuckDB — never rivet. Empty → the client is absent (SKIP upstream).
_store_readback() {
  local store=$1 bkt=$2 pfx=$3 dl="$WORK/dl_${store}_$RANDOM"
  case "$store" in
    s3) duckdb -noheader -list -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='127.0.0.1:9000'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; SELECT count(*) FROM read_parquet('s3://$bkt/$pfx/**/*.parquet')" 2>/dev/null;;
    gcs) # no gsutil needed — pull via the fake-gcs JSON API (independent of rivet).
         mkdir -p "$dl"
         local got; got="$(python3 "$HERE/lib/gcs_pull.py" "http://127.0.0.1:4443" "$bkt" "$pfx" "$dl" 2>/dev/null)"
         [ "${got:-0}" -gt 0 ] || { echo ""; return; }
         duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$dl/**/*.parquet')" 2>/dev/null;;
    azure) command -v az >/dev/null || { echo ""; return; }
         mkdir -p "$dl"; az storage blob download-batch --connection-string "$AZURITE_CONN" -s "$bkt" --pattern "$pfx/*" -d "$dl" >/dev/null 2>&1
         duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$dl/**/*.parquet')" 2>/dev/null;;
  esac
}

_store_up() { case "$1" in
  s3)    curl -s -o /dev/null --max-time 3 http://127.0.0.1:9000/minio/health/live 2>/dev/null;;
  gcs)   curl -s -o /dev/null --max-time 3 http://127.0.0.1:4443/storage/v1/b 2>/dev/null;;
  azure) curl -s -o /dev/null --max-time 3 "http://127.0.0.1:10000/devstoreaccount1?comp=list" 2>/dev/null;;
esac; }

# store destination YAML block (indented under `destination:`).
_store_dest() { local store=$1 bkt=$2 pfx=$3
  case "$store" in
    s3)  printf '      type: s3\n      bucket: %s\n      prefix: %s/\n      region: us-east-1\n      endpoint: http://127.0.0.1:9000\n      access_key_env: MINIO_ACCESS_KEY\n      secret_key_env: MINIO_SECRET_KEY\n' "$bkt" "$pfx";;
    gcs) printf '      type: gcs\n      bucket: %s\n      prefix: %s/\n      endpoint: http://127.0.0.1:4443\n' "$bkt" "$pfx";;
    azure) printf '      type: azure\n      bucket: %s\n      prefix: %s/\n      account_name: devstoreaccount1\n      account_key_env: AZURITE_KEY\n      endpoint: http://127.0.0.1:10000/devstoreaccount1\n' "$bkt" "$pfx";;
    *) return 1;;
  esac
}

# ── gc_survival (store-level): spare in-flight when active, delete true orphan ─
sc_gc_survival() {
  local eng=$1 tag=$2 url=$3
  # Requires a warehouse load target to invoke gc_orphans; that runs in the BQ
  # stage (real load + gc). Here we record it as covered-by-cloud so the ledger is
  # explicit rather than silently absent.
  skip "gc_survival runs in the BigQuery stage (needs a warehouse load target)"
  add "$eng" "$tag" gc_survival - SKIP "covered in BQ stage"
}

# ── BigQuery golden stage (implemented in lib/bigquery.sh) ───────────────────
source "$HERE/lib/bigquery.sh"

# ── CDC end-to-end stage (implemented in lib/cdc.sh) — the change-data-capture
# surface the batch scenarios never exercise: per engine anchor → typed changes →
# capture to a store → INDEPENDENT readback + validate + state population + SQLite-
# vs-Postgres parity + at-least-once crash recovery. Env-driven, SKIP-if-absent.
source "$HERE/lib/cdc.sh"

# ── Release BUILD/PUBLISH path stage (implemented in lib/release_path.sh) — the
# release runs stricter tooling than `cargo build` (cargo-chef manifest parse,
# `publish --locked`, schema regen) that only fails at the TAG, post-publish. Runs
# the real path pre-tag so the mismatch fails loud and local.
source "$HERE/lib/release_path.sh"

# ── Regression vs the PREVIOUS RELEASE (implemented in lib/regression.sh) — the gate
# compares to goldens and to itself, never to the version users run. The new release
# must READ what the previous release WROTE (format-compat) and be no slower / fatter
# than the DOWNLOADED prev-release binary (perf/RSS). Env-driven, SKIP-if-absent.
source "$HERE/lib/regression.sh"
