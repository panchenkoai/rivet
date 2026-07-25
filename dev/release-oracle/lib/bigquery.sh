# ── BigQuery golden stage (sourced by scenarios.sh) ─────────────────────────
# The real-cloud final oracle, made DETERMINISTIC by a checked-in golden. rivet
# exports the type-matrix tables → Parquet → `bq load` → `bq query` reads them back
# → every column value is compared to golden/bigquery_type_matrix.json (the blessed
# warehouse-side representation). A divergence fails the release. Re-bless only on
# purpose with `run.sh --bless-bigquery-golden`.

GOLDEN="$HERE/golden/bigquery_type_matrix.json"

run_bigquery_golden() {
  local proj_env dset_env proj dset
  proj_env="$(cfg bq project_env)"; dset_env="$(cfg bq dataset_env)"
  proj="${!proj_env:-}"; dset="${!dset_env:-}"
  if ! command -v bq >/dev/null; then skip "BigQuery: bq CLI absent"; add bigquery - golden - SKIP "no bq"; return; fi
  if [ -z "$proj" ] || [ -z "$dset" ]; then
    skip "BigQuery: set $proj_env and $dset_env to run the cloud oracle"; add bigquery - golden - SKIP "no creds"; return; fi

  log "BigQuery golden stage ($proj.$dset)"
  # A dedicated PG holds the deterministic type-matrix (hardcoded seed VALUES).
  local url; url="$(bring_up postgres bq postgres:16 55099)"
  [ -z "$url" ] && { skip "BigQuery: type-matrix PG bring-up failed"; add bigquery - golden - SKIP "pg up"; return; }
  seed_engine postgres bq "$url" >/dev/null 2>&1

  local tables; IFS=, read -ra tables <<<"$(cfg bq tables)"
  local blessed="{}" fails=""
  for tbl in "${tables[@]}"; do
    local dir="$WORK/bq_$tbl"; export ORACLE_URL="$url"
    _export_local postgres "$url" "$tbl" "$dir" full || { fails+="$tbl:export "; continue; }
    local pq; pq="$(ls "$dir"/**/*.parquet "$dir"/*.parquet 2>/dev/null | head -1)"
    [ -z "$pq" ] && { fails+="$tbl:no-parquet "; continue; }
    local bqt="oracle_${tbl}"
    bq --project_id="$proj" load --replace --source_format=PARQUET "$dset.$bqt" "$pq" >/dev/null 2>&1 \
      || { fails+="$tbl:bq-load "; continue; }
    # read back deterministically (ORDER BY id, JSON), normalize with python.
    local got; got="$(bq --project_id="$proj" query --nouse_legacy_sql --format=prettyjson \
      "SELECT * FROM \`$proj.$dset.$bqt\` ORDER BY id" 2>/dev/null \
      | python3 "$HERE/lib/normalize_bq.py")"
    [ -z "$got" ] && { fails+="$tbl:readback "; continue; }
    if [ "$BLESS" = 1 ]; then
      blessed="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); d['$tbl']=json.loads(sys.argv[2]); print(json.dumps(d,indent=2,sort_keys=True))" "$blessed" "$got")"
    else
      local want; want="$(python3 -c "import json,sys; print(json.dumps(json.load(open('$GOLDEN')).get('$tbl'),sort_keys=True))" 2>/dev/null)"
      if [ "$(python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$got")" = "$want" ]; then
        ok "BigQuery $tbl matches golden"; add bigquery - "golden:$tbl" - PASS
      else
        bad "BigQuery $tbl DIVERGED from golden — rivet type export or BQ mapping changed"
        add bigquery - "golden:$tbl" - FAIL "diverged"; fails+="$tbl:diverge "
      fi
    fi
  done

  if [ "$BLESS" = 1 ]; then
    mkdir -p "$HERE/golden"; echo "$blessed" > "$GOLDEN"
    ok "BLESSED golden → $GOLDEN"; add bigquery - golden - PASS "blessed"
  fi
}
