#!/usr/bin/env bash
# Run full CLI matrix against PG and MySQL. Save stdout/stderr/exit code
# per scenario into logs/matrix/<id>/.  Summary line per scenario printed
# to terminal.  Designed to be re-runnable: prior state DBs get wiped.
#
# Naming: <engine>_<cmd>_<variant>  e.g. pg_run_full, my_plan_chunked.

set -u
# Default: run in-place from `dev/cli_matrix/` in the repo, pulling
# rivet from the release build.  Override via env if a copy of the
# script lives elsewhere (e.g. ad-hoc runs from /tmp).
ROOT="${ROOT_OVERRIDE:-$(cd "$(dirname "$0")" && pwd)}"
R="${RIVET_BIN:-$ROOT/rivet}"
LM="$ROOT/logs/matrix"
mkdir -p "$LM"
if [[ ! -x "$R" ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build it first (cargo build --bin rivet --release) and copy or symlink" >&2
  echo "  cp target/release/rivet dev/cli_matrix/rivet" >&2
  exit 2
fi

export PG_URL="postgresql://rivet:rivet@127.0.0.1:5432/rivet"
export MY_URL="mysql://rivet:rivet@127.0.0.1:3306/rivet"
# DATABASE_URL is what `rivet init` defaults to (no --source-env).
export DATABASE_URL="$PG_URL"

run() {
  local id="$1"; shift
  local desc="$1"; shift
  [[ "$1" == "--" ]] && shift
  local dir="$LM/$id"
  mkdir -p "$dir"
  printf '%s\n' "$desc" > "$dir/description"
  printf '%s ' "$@" > "$dir/cmd"; printf '\n' >> "$dir/cmd"
  "$@" > "$dir/stdout" 2> "$dir/stderr"
  local rc=$?
  printf '%s\n' "$rc" > "$dir/exit_code"
  local osize=$(wc -c < "$dir/stdout" | tr -d ' ')
  local esize=$(wc -c < "$dir/stderr" | tr -d ' ')
  printf '%-30s rc=%-3s stdout=%-6s stderr=%-6s  %s\n' "$id" "$rc" "$osize" "$esize" "$desc"
}

# Per-engine config dirs so state DBs do not collide (F2 from audit).
mkdir -p "$ROOT/cfg/pg" "$ROOT/cfg/my"
cp "$ROOT"/cfg/pg_*.yaml "$ROOT/cfg/pg/" 2>/dev/null
cp "$ROOT"/cfg/my_*.yaml "$ROOT/cfg/my/" 2>/dev/null
rm -f "$ROOT"/cfg/pg/.rivet_state.db* "$ROOT"/cfg/my/.rivet_state.db*

PGC="$ROOT/cfg/pg/pg_full.yaml"
PGI="$ROOT/cfg/pg/pg_incremental.yaml"
PGK="$ROOT/cfg/pg/pg_chunked.yaml"
PGP="$ROOT/cfg/pg/pg_param.yaml"
MYC="$ROOT/cfg/my/my_full.yaml"
MYI="$ROOT/cfg/my/my_incremental.yaml"
MYK="$ROOT/cfg/my/my_chunked.yaml"

echo "================== doctor =================="
run pg_doctor_ok       "doctor PG happy"          -- "$R" doctor -c "$PGC"
run my_doctor_ok       "doctor MySQL happy"       -- "$R" doctor -c "$MYC"
run pg_doctor_unset    "doctor PG with env unset" -- env -u PG_URL "$R" doctor -c "$PGC"

echo "================== check =================="
run pg_check_full      "check PG full"            -- "$R" check -c "$PGC"
run my_check_full      "check MySQL full"         -- "$R" check -c "$MYC"
run pg_check_incr      "check PG incremental"     -- "$R" check -c "$PGI"
run pg_check_chunked   "check PG chunked"         -- "$R" check -c "$PGK"
run pg_check_unknown   "check PG --export unknown" -- "$R" check -c "$PGC" --export does_not_exist
run pg_check_type_report "check PG --type-report"  -- "$R" check -c "$PGC" --type-report
run pg_check_type_report_json "check PG --type-report --json" -- "$R" check -c "$PGC" --json
run pg_check_strict    "check PG --strict"        -- "$R" check -c "$PGC" --strict
run pg_check_target_bq "check PG --target bigquery" -- "$R" check -c "$PGC" --target bigquery

echo "================== run =================="
run pg_run_full        "run PG full"                       -- "$R" run -c "$PGC"
run pg_run_incr        "run PG incremental"               -- "$R" run -c "$PGI"
run pg_run_chunked     "run PG chunked"                    -- "$R" run -c "$PGK"
run pg_run_validate    "run PG full --validate"           -- "$R" run -c "$PGC" --validate
run pg_run_reconcile   "run PG full --reconcile"          -- "$R" run -c "$PGC" --reconcile
run pg_run_val_rec     "run PG full --validate --reconcile" -- "$R" run -c "$PGC" --validate --reconcile
run pg_run_json        "run PG full --json"                -- "$R" run -c "$PGC" --json
run pg_run_summary     "run PG full --summary-output ..."  -- "$R" run -c "$PGC" --summary-output "$ROOT/logs/matrix/pg_run_summary/agg.json"
run pg_run_force_no_resume "run PG --force without --resume" -- "$R" run -c "$PGC" --force
run pg_run_resume_no_chk   "run PG --resume without chunk_checkpoint" -- "$R" run -c "$PGC" --resume
run pg_run_parallel_single "run PG --parallel-exports (single export)" -- "$R" run -c "$PGC" --parallel-exports
run pg_run_unknown     "run PG --export unknown"           -- "$R" run -c "$PGC" --export does_not_exist
run pg_run_idempotent  "run PG full SECOND time (idempotency)" -- "$R" run -c "$PGC"

run my_run_full        "run MySQL full"                    -- "$R" run -c "$MYC"
run my_run_incr        "run MySQL incremental"             -- "$R" run -c "$MYI"
run my_run_chunked     "run MySQL chunked"                 -- "$R" run -c "$MYK"
run my_run_validate    "run MySQL full --validate"         -- "$R" run -c "$MYC" --validate
run my_run_reconcile   "run MySQL full --reconcile"        -- "$R" run -c "$MYC" --reconcile
run my_run_json        "run MySQL full --json"             -- "$R" run -c "$MYC" --json

echo "================== plan =================="
run pg_plan_full       "plan PG full --format json -o"    -- "$R" plan -c "$PGC" -e pa_audit --format json -o "$LM/pg_plan_full/plan.json"
run pg_plan_incr       "plan PG incremental --format json -o" -- "$R" plan -c "$PGI" -e pa_audit --format json -o "$LM/pg_plan_incr/plan.json"
run pg_plan_chunked    "plan PG chunked --format json -o" -- "$R" plan -c "$PGK" -e pa_audit --format json -o "$LM/pg_plan_chunked/plan.json"
run pg_plan_pretty     "plan PG full --format pretty"     -- "$R" plan -c "$PGC" -e pa_audit --format pretty
run pg_plan_json_stdout "plan PG full --format json (no -o)" -- "$R" plan -c "$PGC" -e pa_audit --format json
run pg_plan_default    "plan PG full (no flags)"          -- "$R" plan -c "$PGC" -e pa_audit
run pg_plan_unknown    "plan PG --export unknown"         -- "$R" plan -c "$PGC" -e does_not_exist --format json
run pg_plan_bad_format "plan PG --format invalid"         -- "$R" plan -c "$PGC" -e pa_audit --format invalid
run pg_plan_json_errors "plan PG --json-errors"           -- "$R" --json-errors plan -c "$PGC" -e does_not_exist --format json
run pg_plan_param_used  'plan PG with --param matching ${max_id} placeholder' -- "$R" plan -c "$PGP" -e pa_audit --param max_id=20 --format json
run pg_plan_param_unused 'plan PG with --param unused (warning expected once)'    -- "$R" plan -c "$PGC" -e pa_audit --param unused=value --format json

run my_plan_full       "plan MySQL full --format json -o" -- "$R" plan -c "$MYC" -e pa_audit --format json -o "$LM/my_plan_full/plan.json"
run my_plan_incr       "plan MySQL incremental --format json -o" -- "$R" plan -c "$MYI" -e pa_audit --format json -o "$LM/my_plan_incr/plan.json"
run my_plan_chunked    "plan MySQL chunked --format json -o" -- "$R" plan -c "$MYK" -e pa_audit --format json -o "$LM/my_plan_chunked/plan.json"

echo "================== apply =================="
# Re-use plans from plan step
run pg_apply_full      "apply PG full"            -- "$R" apply "$LM/pg_plan_full/plan.json"
run pg_apply_incr      "apply PG incremental"     -- "$R" apply "$LM/pg_plan_incr/plan.json"
run pg_apply_chunked   "apply PG chunked"         -- "$R" apply "$LM/pg_plan_chunked/plan.json"
run pg_apply_missing   "apply missing file"       -- "$R" apply /tmp/no_such_plan_xyz.json
run pg_apply_no_arg    "apply (no arg)"           -- "$R" apply
# Build derived stale + drifted from pg_plan_incr
jq '.created_at = "2020-01-01T00:00:00Z" | .expires_at = "2020-01-02T00:00:00Z"' "$LM/pg_plan_incr/plan.json" > "$LM/_stale.json"
jq '.computed.cursor_snapshot = "99999"' "$LM/pg_plan_incr/plan.json" > "$LM/_drifted.json"
echo "not json {" > "$LM/_corrupt.json"
run pg_apply_stale_no_force  "apply stale (no --force)"   -- "$R" apply "$LM/_stale.json"
run pg_apply_stale_force     "apply --force stale"        -- "$R" apply --force "$LM/_stale.json"
run pg_apply_drift_no_force  "apply drifted (no --force)" -- "$R" apply "$LM/_drifted.json"
run pg_apply_drift_force     "apply --force drifted (F1)" -- "$R" apply --force "$LM/_drifted.json"
run pg_apply_corrupt   "apply corrupt JSON"     -- "$R" apply "$LM/_corrupt.json"
run pg_apply_json_errors "apply --json-errors"  -- "$R" --json-errors apply /tmp/no_such_plan_xyz.json

run my_apply_full      "apply MySQL full"       -- "$R" apply "$LM/my_plan_full/plan.json"
run my_apply_incr      "apply MySQL incremental" -- "$R" apply "$LM/my_plan_incr/plan.json"
run my_apply_chunked   "apply MySQL chunked"    -- "$R" apply "$LM/my_plan_chunked/plan.json"
# Frozen v0.7.5 plan must still apply with --force (stale by definition once
# committed). Catches plan-schema regressions visible only at apply-time.
run pg_apply_legacy_frozen "apply frozen v0.7.5 plan --force" -- "$R" apply --force /Users/andriipanchenko/rivet/tests/fixtures/artifacts_legacy/v0_7_5_plan_full.json

echo "================== state =================="
run pg_state_help      "state --help"           -- "$R" state --help
run pg_state_files     "state files (PG cfg)"   -- "$R" state files -c "$PGC"
run pg_state_progression "state progression"    -- "$R" state progression -c "$PGC"
run my_state_files     "state files (MySQL cfg)" -- "$R" state files -c "$MYC"
run pg_state_show      "state show (PG)"        -- "$R" state show -c "$PGC"
run pg_state_reset     "state reset (PG)"       -- "$R" state reset -c "$PGC" --export pa_audit
run pg_state_reset_unknown "state reset (unknown export)" -- "$R" state reset -c "$PGC" --export nope
run pg_state_reset_chunks_stuck "state reset-chunks --stuck-checkpoints" -- "$R" state reset-chunks -c "$PGK" --stuck-checkpoints
run pg_state_files_export "state files --export pa_audit" -- "$R" state files -c "$PGC" --export pa_audit --last 5

echo "================== metrics =================="
run pg_metrics         "metrics PG"             -- "$R" metrics -c "$PGC"
run pg_metrics_export  "metrics PG --export"    -- "$R" metrics -c "$PGC" --export pa_audit
run my_metrics         "metrics MySQL"          -- "$R" metrics -c "$MYC"

echo "================== schema =================="
run schema_help        "schema --help"          -- "$R" schema --help
run schema_config      "schema config"          -- "$R" schema config

echo "================== journal =================="
run pg_journal         "journal PG"             -- "$R" journal -c "$PGC" --export pa_audit
run my_journal         "journal MySQL"          -- "$R" journal -c "$MYC" --export pa_audit
run pg_journal_last1   "journal PG --last 1"    -- "$R" journal -c "$PGC" --export pa_audit --last 1
run pg_journal_unknown_export "journal PG --export unknown" -- "$R" journal -c "$PGC" --export does_not_exist
run pg_journal_missing_run_id "journal PG --run-id missing" -- "$R" journal -c "$PGC" --export pa_audit --run-id RIVET_NO_SUCH_RUN_ID

echo "================== validate =================="
run pg_validate        "validate PG"            -- "$R" validate -c "$PGC"
run my_validate        "validate MySQL"         -- "$R" validate -c "$MYC"

echo "================== reconcile =================="
run pg_reconcile       "reconcile PG (no chunk_checkpoint)" -- "$R" reconcile -c "$PGC"
run pg_reconcile_chk   "reconcile PG chunked"   -- "$R" reconcile -c "$PGK"

echo "================== repair =================="
run pg_repair          "repair PG"              -- "$R" repair -c "$PGK"

echo "================== init =================="
mkdir -p "$LM/_init"
run pg_init_table      "init PG --table users"  -- "$R" init --source-env PG_URL --table users -o "$LM/_init/pg_users.yaml"
run my_init_table      "init MySQL --table users" -- "$R" init --source-env MY_URL --table users -o "$LM/_init/my_users.yaml"
run pg_init_unset      "init env unset"         -- env -u PG_URL "$R" init --source-env PG_URL --table users -o "$LM/_init/pg_unset.yaml"
run pg_init_no_table   "init no --table"        -- "$R" init --source-env PG_URL -o "$LM/_init/pg_no_table.yaml"

echo
echo "DONE.  $(ls "$LM" | grep -v '^_' | wc -l | tr -d ' ') scenarios captured in $LM"
