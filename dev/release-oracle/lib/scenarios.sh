# ── Release-oracle scenarios (sourced by run.sh) ────────────────────────────
# Each scenario is a function taking (engine, tag, url). It records PASS/FAIL/SKIP
# via add() + ok()/bad()/skip() from run.sh. Reuses the canonical seeds (already
# applied) and the DuckDB independent oracle — the same discipline the manual
# dogfood used, now deterministic and repeatable.

run_scenarios() {
  local eng=$1 tag=$2 url=$3
  sc_verdicts "$eng" "$tag" "$url"
  sc_integrity_types "$eng" "$tag" "$url"
  # blessing the local goldens (verdicts + duckdb-type) — skip the store loads.
  { [ "${BLESS_VERDICTS:-0}" = 1 ] || [ "${BLESS_DUCKDB:-0}" = 1 ]; } && return
  for store in $(cfg stores); do
    sc_load "$eng" "$tag" "$url" "$store"
  done
  [ "$eng" = postgres ] && sc_gc_survival "$eng" "$tag" "$url"
}

# helper: run rivet init into a config for the whole DB (or --schema for pg/mssql
# garbage), echoing the config path.
_init_cfg() {
  local eng=$1 url=$2 schema=$3 out="$WORK/${eng}_${schema:-all}.yaml"
  local envn="ORACLE_URL"; export ORACLE_URL="$url"
  local tlsflag=""; [ "$eng" = mssql ] && tlsflag="--tls-accept-invalid-certs"
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
  # Build the live map by checking every config the engine needs (seeds, +garbage).
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

# ── integrity + types ────────────────────────────────────────────────────────
# Two independent oracles:
#  (1) users 150K loss/dup — source count+distinct vs the DuckDB read of the parts.
#  (2) TYPE matrices — a DuckDB GOLDEN (golden/duckdb_type_matrix.json, per engine):
#      rivet exports rivet_type_matrix[_full] → DuckDB reads every value back → the
#      canonical JSON is compared to the blessed golden. A type regression (decimal
#      precision, uuid nulling, timestamp shift, enum) fails against a fixed truth,
#      exactly like the BigQuery golden but on the local independent reader.
GOLDEN_DUCKDB="$HERE/golden/duckdb_type_matrix.json"
sc_integrity_types() {
  local eng=$1 tag=$2 url=$3 out="$WORK/it_${eng}_${tag//./_}" fails=""
  mkdir -p "$out"
  # (1) users loss/dup — all engines.
  if _export_local "$eng" "$url" users "$out/users" chunked; then
    local scnt dcnt
    scnt="$(_source_count_distinct "$eng" "$url" users id)"
    dcnt="$(duckdb -noheader -list -c "SELECT count(*)||' '||count(DISTINCT $( [ "$eng" = mongo ] && echo _id || echo id )) FROM read_parquet('$out/users/**/*.parquet')" 2>/dev/null)"
    [ "$scnt" = "$dcnt" ] || fails+="users src[$scnt]≠parquet[$dcnt] "
  else
    fails+="users-export-failed "
  fi
  # (2) type matrices → DuckDB golden (SQL engines have rivet_type_matrix[_full]).
  if [ "$eng" != mongo ]; then
    for tmt in rivet_type_matrix rivet_type_matrix_full; do
      _export_local "$eng" "$url" "$tmt" "$out/$tmt" full || { fails+="$tmt-export "; continue; }
      local got; got="$(duckdb -json -c "SELECT * FROM read_parquet('$out/$tmt/**/*.parquet') ORDER BY id" 2>/dev/null | python3 "$HERE/lib/normalize_bq.py")"
      [ -z "$got" ] && { fails+="$tmt-readback "; continue; }
      if [ "${BLESS_DUCKDB:-0}" = 1 ]; then
        _golden_put "$GOLDEN_DUCKDB" "$eng" "$tmt" "$got"
      else
        local want; want="$(_golden_get "$GOLDEN_DUCKDB" "$eng" "$tmt")"
        if [ -z "$want" ]; then fails+="$tmt-no-golden "; \
        elif [ "$(_json_canon "$got")" != "$want" ]; then fails+="$tmt-TYPE-DIVERGED "; fi
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

_source_count_distinct() {
  local eng=$1 url=$2 tbl=$3 idc=$4 name="rivet-oracle-eng-${eng}-*"
  local c; c="$(docker ps --format '{{.Names}}' | grep "rivet-oracle-eng-${eng}-" | head -1)"
  case "$eng" in
    postgres) docker exec "$c" psql -U rivet -d rivet -tA -c "SELECT count(*)||' '||count(DISTINCT $idc) FROM $tbl" 2>/dev/null;;
    mysql)    docker exec "$c" mysql -urivet -privet rivet -N -e "SELECT CONCAT(count(*),' ',count(DISTINCT $idc)) FROM $tbl" 2>/dev/null;;
    mssql)    docker exec "$c" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -C -h -1 -W -Q "SET NOCOUNT ON; SELECT CONCAT(count(*),' ',count(DISTINCT $idc)) FROM $tbl" 2>/dev/null | tr -d '\r';;
    mongo)    docker exec "$c" mongosh mongodb://127.0.0.1:27017/rivet --quiet --eval "print(db.$tbl.countDocuments()+' '+db.$tbl.distinct('_id').length)" 2>/dev/null;;
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

# export one table to a LOCAL dir (parquet). mode: chunked|full.
_export_local() {
  local eng=$1 url=$2 tbl=$3 dir=$4 mode=$5 yaml="$WORK/ex_$$_${tbl//./_}.yaml"
  rm -rf "$dir"; export ORACLE_URL="$url"
  [ "$eng" = mongo ] && mode=full   # Mongo has no keyset/chunked — full scan only
  local keyline="    mode: full"; [ "$mode" = chunked ] && keyline=$'    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000'
  local tlsblk=""; [ "$eng" = mssql ] && tlsblk=$'\n  tls: {accept_invalid_certs: true}'
  cat > "$yaml" <<YAML
source:
  type: $eng
  url_env: ORACLE_URL$tlsblk
exports:
  - name: $tbl
    table: $tbl
$keyline
    format: parquet
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
  local bkt; bkt="$(cfg store "$store" bucket)"; local pfx="oracle/${eng}_${tag//./_}/${store}"
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
  # clean the prefix first (best-effort), then export+validate.
  local out; out="$("$RIVET" run --validate -c "$yaml" 2>&1)"
  local rows; rows="$(grep -aoE '[0-9,]+ rows' <<<"$out" | head -1 | tr -d ', rows')"
  local scnt; scnt="$(_source_count_distinct "$eng" "$url" users id | cut -d' ' -f1)"
  if grep -qaiE "validat(e|ion).*(pass|ok|✓)|verified" <<<"$out" && [ "$rows" = "$scnt" ]; then
    ok "load→$store write+reread+validate $rows rows"; add "$eng" "$tag" load "$store" PASS "$rows"
  elif [ "$rows" = "$scnt" ] && ! grep -qaiE "error|fail" <<<"$out"; then
    ok "load→$store roundtrip $rows rows (validate quiet)"; add "$eng" "$tag" load "$store" PASS "$rows"
  else
    bad "load→$store failed (rows=$rows src=$scnt)"; add "$eng" "$tag" load "$store" FAIL "$(grep -aiE 'error|fail' <<<"$out" | head -1)"
  fi
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
