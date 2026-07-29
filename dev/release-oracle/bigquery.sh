# ── BigQuery golden stage (sourced by scenarios.sh) ─────────────────────────
# The real-cloud final oracle, made DETERMINISTIC by a checked-in golden. The load
# goes through rivet on BOTH legs — `rivet run` stages the type-matrix parts to a
# REAL GCS bucket, `rivet load` loads them GCS → BigQuery — then `bq query` (gcloud,
# an INDEPENDENT reader) reads them back and every column value is compared to
# golden/bigquery_type_matrix.json (the blessed warehouse-side representation). A
# divergence fails the release. Re-bless only on purpose with
# `run.sh --bless-bigquery-golden`.

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
  echo "BQDBG url=$url" >&2
  local serr; serr="$(seed_engine postgres bq "$url" 2>&1)"; echo "BQDBG seed_err=[$serr]" >&2

  local tables; IFS=, read -ra tables <<<"$(cfg bq tables)"
  # BigQuery loads from gs:// — a REAL GCS staging bucket (not fake-gcs). Reuse a
  # known test bucket; the stage stages parts there, `rivet load`s them, cleans up.
  local bucket="${BQ_ORACLE_BUCKET:-rivet_data_test}"
  local blessed="{}" fails=""
  for tbl in "${tables[@]}"; do
    export ORACLE_URL="$url"
    local pfx="release-oracle/bq/${tbl}" cfg="$WORK/bqload_${tbl}.yaml"
    cat > "$cfg" <<YAML
source:
  type: postgres
  url_env: ORACLE_URL
exports:
  - name: $tbl
    table: $tbl
    mode: full
    format: parquet
    destination: {type: gcs, bucket: $bucket, prefix: $pfx/}
load:
  target: bigquery
  project: $proj
  dataset: $dset
YAML
    gcloud storage rm -r "gs://$bucket/$pfx" >/dev/null 2>&1 || true
    local ro lo; ro="$("$RIVET" run  -c "$cfg" 2>&1)" || { echo "BQDBG run FAIL $tbl: $(tail -2 <<<"$ro")" >&2; fails+="$tbl:export "; continue; }
    lo="$("$RIVET" load -c "$cfg" 2>&1)" || { echo "BQDBG load FAIL $tbl: $(tail -3 <<<"$lo")" >&2; fails+="$tbl:rivet-load "; continue; }
    echo "BQDBG $tbl run+load OK" >&2
    # read back deterministically via bq (gcloud), normalize with python.
    local got; got="$(bq --project_id="$proj" query --nouse_legacy_sql --format=prettyjson \
      "SELECT * FROM \`$proj.$dset.$tbl\` ORDER BY id" 2>/dev/null \
      | python3 "$HERE/lib/normalize_bq.py")"
    echo "BQDBG $tbl readback rows=$(python3 -c "import json,sys;print(len(json.loads(sys.argv[1])))" "$got" 2>/dev/null || echo PARSE_ERR)" >&2
    gcloud storage rm -r "gs://$bucket/$pfx" >/dev/null 2>&1 || true
    bq --project_id="$proj" rm -f -t "$dset.$tbl" >/dev/null 2>&1 || true
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
