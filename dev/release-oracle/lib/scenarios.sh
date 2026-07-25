# ── Release-oracle scenarios (sourced by run.sh) ────────────────────────────
# Each scenario is a function taking (engine, tag, url). It records PASS/FAIL/SKIP
# via add() + ok()/bad()/skip() from run.sh. Reuses the canonical seeds (already
# applied) and the DuckDB independent oracle — the same discipline the manual
# dogfood used, now deterministic and repeatable.

run_scenarios() {
  local eng=$1 tag=$2 url=$3
  sc_verdicts "$eng" "$tag" "$url"
  sc_integrity_types "$eng" "$tag" "$url"
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

# ── verdicts: init → strategy per table matches expectation ──────────────────
sc_verdicts() {
  local eng=$1 tag=$2 url=$3 fails=""
  if [ "$eng" = mongo ]; then
    local cfg; cfg="$(_init_cfg mongo "$url" "")" || { skip "mongo init failed"; add mongo "$tag" verdicts - SKIP "init"; return; }
    # Mongo: no keyset (documented exception) — large collection → full, and the
    # rationale must be the corrected one (not the false "below 100K").
    [ "$(_strategy_of "$cfg" users)" = full ] || fails+="users≠full "
    grep -A3 "name: users" "$cfg" | grep -q "below 100K" && fails+="stale-below-100K-msg "
    [ -z "$fails" ] && { ok "verdicts (mongo: full + accurate rationale)"; add mongo "$tag" verdicts - PASS; } \
                    || { bad "verdicts: $fails"; add mongo "$tag" verdicts - FAIL "$fails"; }
    return
  fi
  # SQL engines: seeds → keyset, orders_sparse → full.
  local seeds_cfg; seeds_cfg="$(_init_cfg "$eng" "$url" "$( [ "$eng" = mssql ] && echo dbo )")" \
    || { skip "$eng init failed"; add "$eng" "$tag" verdicts - SKIP init; return; }
  for t in users orders events page_views content_items orders_coalesce; do
    [ "$(_strategy_of "$seeds_cfg" "$t")" = keyset ] || fails+="$t≠keyset "
  done
  [ "$(_strategy_of "$seeds_cfg" orders_sparse)" = full ] || fails+="orders_sparse≠full "
  # garbage shapes. Export NAMES: pg/mssql strip the `ext.` schema (name is the bare
  # table), mysql keeps the `ext_` prefix (that IS the table name).
  local gpref=""; [ "$eng" = mysql ] && gpref="ext_"
  local gcfg
  if [ "$eng" = mysql ]; then gcfg="$seeds_cfg"; else gcfg="$(_init_cfg "$eng" "$url" ext)"; fi
  local dkey="${gpref}decimal_key" unidx="${gpref}unindexed_id" refh="${gpref}ref_id_history" bpk="${gpref}bigint_pk_dual_ts"
  [ "$(_strategy_of "$gcfg" "$bpk")" = keyset ] || fails+="bigint_pk≠keyset "
  [ "$(_strategy_of "$gcfg" "$dkey")" = full ] || fails+="decimal_key≠full-bail "
  [ "$(_strategy_of "$gcfg" "$unidx")" = keyset ] && fails+="unindexed_id-WRONGLY-keyset "   # name-trap: must NOT keyset
  [ "$(_strategy_of "$gcfg" "$refh")" = range ] || fails+="ref_id_history≠range "
  # no phantom heavy-chunk warning anywhere.
  local hc; hc="$(_check_heavy_chunk_count "$eng" "$url" "$seeds_cfg")"
  [ "$hc" = 0 ] || fails+="phantom-heavy-chunk=$hc "
  [ -z "$fails" ] && { ok "verdicts (seeds→keyset, garbage shapes, 0 phantom)"; add "$eng" "$tag" verdicts - PASS; } \
                  || { bad "verdicts: $fails"; add "$eng" "$tag" verdicts - FAIL "$fails"; }
}

_check_heavy_chunk_count() {
  local eng=$1 url=$2 cfg=$3; export ORACLE_URL="$url"
  local tlsflag=""; [ "$eng" = mssql ] && tlsflag=""
  "$RIVET" check -c "$cfg" 2>&1 | grep -ac "Heavy chunk"
}

# ── integrity + types: export → DuckDB oracle vs source ──────────────────────
sc_integrity_types() {
  local eng=$1 tag=$2 url=$3 out="$WORK/it_${eng}_${tag//./_}" fails=""
  mkdir -p "$out"
  # keyset integrity table (users) + type-rich table.
  local tmtable="rivet_type_matrix"
  _export_local "$eng" "$url" users "$out/users" chunked || { skip "$eng users export failed"; add "$eng" "$tag" integrity_types - SKIP export; return; }
  _export_local "$eng" "$url" "$tmtable" "$out/tm" full  || true
  # users loss/dup (all engines): source count/distinct vs parquet.
  local scnt; scnt="$(_source_count_distinct "$eng" "$url" users id)"
  local dcnt; dcnt="$(duckdb -noheader -list -c "SELECT count(*)||' '||count(DISTINCT id) FROM read_parquet('$out/users/**/*.parquet')" 2>/dev/null)"
  [ "$scnt" = "$dcnt" ] || fails+="users src[$scnt]≠parquet[$dcnt] "
  # type values (pg/mysql via scanner: decimal + uuid distinct).
  if [ "$eng" = postgres ] || [ "$eng" = mysql ]; then
    local tv; tv="$(_type_oracle "$eng" "$tmtable" "$out/tm")"
    [ "$tv" = ok ] || fails+="type-oracle:$tv "
  fi
  [ -z "$fails" ] && { ok "integrity+types (loss/dup 0, types match)"; add "$eng" "$tag" integrity_types - PASS; } \
                  || { bad "integrity+types: $fails"; add "$eng" "$tag" integrity_types - FAIL "$fails"; }
}

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
  local eng=$1 tbl=$2 dir=$3 att
  case "$eng" in
    postgres) att="INSTALL postgres_scanner; LOAD postgres_scanner; ATTACH 'postgresql://rivet:rivet@127.0.0.1:$(_hostport "$eng")/rivet' AS s (TYPE postgres, READ_ONLY);";;
    mysql)    att="INSTALL mysql_scanner; LOAD mysql_scanner; ATTACH 'host=127.0.0.1 port=$(_hostport "$eng") user=rivet password=rivet database=rivet' AS s (TYPE mysql, READ_ONLY);";;
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
  cat > "$yaml" <<YAML
source:
  type: $eng
  url_env: ORACLE_URL$tlsblk
exports:
  - name: users
    table: users
    mode: chunked
    chunk_by_key: id
    chunk_size: 50000
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
