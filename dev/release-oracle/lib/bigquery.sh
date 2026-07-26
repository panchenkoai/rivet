# ── BigQuery golden stage (sourced by scenarios.sh) ─────────────────────────
# The real-cloud final oracle, made DETERMINISTIC by a per-engine checked-in
# golden. For EVERY SQL engine (postgres / mysql / mssql — Mongo has no type
# matrix) the load goes through rivet on BOTH legs: `rivet run` stages that
# engine's `rivet_type_matrix` to a REAL GCS bucket, `rivet load` loads it GCS →
# BigQuery, then `bq query` (gcloud, an INDEPENDENT reader) reads it back and
# every column value is compared to golden/bigquery_type_matrix.json
# (`{engine: {rivet_type_matrix: [...]}}`). A divergence fails the release.
# Re-bless only on purpose with `run.sh --bless-bigquery-golden`.

GOLDEN="$HERE/golden/bigquery_type_matrix.json"

run_bigquery_golden() {
  local proj_env dset_env proj dset
  proj_env="$(cfg bq project_env)"; dset_env="$(cfg bq dataset_env)"
  proj="${!proj_env:-}"; dset="${!dset_env:-}"
  if ! command -v bq >/dev/null; then skip "BigQuery: bq CLI absent"; add bigquery - golden - SKIP "no bq"; return; fi
  if [ -z "$proj" ] || [ -z "$dset" ]; then
    skip "BigQuery: set $proj_env and $dset_env to run the cloud oracle"; add bigquery - golden - SKIP "no creds"; return; fi

  log "BigQuery golden stage ($proj.$dset) — per SQL engine"
  local bucket="${BQ_ORACLE_BUCKET:-rivet_data_test}"
  local matrix; matrix="$(cfg bq tables)"   # the single comprehensive matrix name
  local blessed="{}"

  for eng in $(cfg engines); do
    [ "$eng" = mongo ] && continue          # no type matrix (full-scan only)
    local port; case "$eng" in postgres) port=55099;; mysql) port=55098;; mssql) port=55097;; *) continue;; esac
    local image; image="$(cfg versions "$eng" | head -1 | awk '{print $2}')"
    local url; url="$(bring_up "$eng" bq "$image" "$port")"
    [ -z "$url" ] && { skip "BigQuery[$eng]: bring-up failed"; add bigquery "$eng" golden - SKIP "bring-up"; continue; }
    if [ -n "$(seed_engine "$eng" bq "$url")" ]; then
      skip "BigQuery[$eng]: seed errors"; add bigquery "$eng" golden - SKIP "seed"
      [ "$KEEP" = 1 ] || docker rm -f "rivet-oracle-eng-${eng}-bq" >/dev/null 2>&1; continue; fi
    export ORACLE_URL="$url"
    local tlsblk=""; [ "$eng" = mssql ] && tlsblk=$'\n  tls: {accept_invalid_certs: true}'
    # rivet load names the BigQuery table after the `table:` field, so every engine
    # lands in the SAME `rivet_type_matrix` table — fine because engines run
    # SEQUENTIALLY here (load → read back → rm, before the next engine).
    local exp="$matrix"
    local pfx="release-oracle/bq/${eng}"
    local cfgf="$WORK/bqload_${eng}.yaml"
    cat > "$cfgf" <<YAML
source:
  type: $eng
  url_env: ORACLE_URL$tlsblk
exports:
  - name: $exp
    table: $matrix
    mode: full
    format: parquet
    destination: {type: gcs, bucket: $bucket, prefix: $pfx/}
load:
  target: bigquery
  project: $proj
  dataset: $dset
YAML
    gcloud storage rm -r "gs://$bucket/$pfx" >/dev/null 2>&1 || true
    local got=""
    if "$RIVET" run -c "$cfgf" >/dev/null 2>&1 && "$RIVET" load -c "$cfgf" >/dev/null 2>&1; then
      got="$(bq --project_id="$proj" query --nouse_legacy_sql --format=prettyjson \
        "SELECT * FROM \`$proj.$dset.$exp\` ORDER BY id" 2>/dev/null | python3 "$HERE/lib/normalize_bq.py")"
    else
      bad "BigQuery[$eng]: rivet run/load failed"; add bigquery "$eng" golden - FAIL "load"
    fi
    gcloud storage rm -r "gs://$bucket/$pfx" >/dev/null 2>&1 || true
    bq --project_id="$proj" rm -f -t "$dset.$exp" >/dev/null 2>&1 || true
    [ "$KEEP" = 1 ] || docker rm -f "rivet-oracle-eng-${eng}-bq" >/dev/null 2>&1
    [ -z "$got" ] && continue
    if [ "$BLESS" = 1 ]; then
      blessed="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); d.setdefault(sys.argv[2],{})[sys.argv[3]]=json.loads(sys.argv[4]); print(json.dumps(d,indent=2,sort_keys=True))" "$blessed" "$eng" "$matrix" "$got")"
      ok "BigQuery[$eng] blessed ($(python3 -c "import json,sys;print(len(json.loads(sys.argv[1])))" "$got") rows)"; add bigquery "$eng" golden - PASS blessed
    else
      local want; want="$(python3 -c "import json,os,sys; g=json.load(open('$GOLDEN')) if os.path.exists('$GOLDEN') else {}; v=g.get(sys.argv[1],{}).get(sys.argv[2]); print(json.dumps(v,sort_keys=True) if v is not None else '')" "$eng" "$matrix")"
      if [ -n "$want" ] && [ "$(python3 -c "import json,sys;print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$got")" = "$want" ]; then
        ok "BigQuery[$eng] matches golden"; add bigquery "$eng" golden - PASS
      else
        bad "BigQuery[$eng] DIVERGED from golden — rivet type export or BQ mapping changed"
        add bigquery "$eng" golden - FAIL "diverged"
      fi
    fi
  done

  if [ "$BLESS" = 1 ]; then
    mkdir -p "$HERE/golden"; echo "$blessed" > "$GOLDEN"
    ok "BLESSED BQ golden (per engine) → $GOLDEN"; add bigquery - golden - PASS "blessed"
  fi
}
