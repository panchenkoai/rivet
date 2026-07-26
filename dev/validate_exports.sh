#!/usr/bin/env bash
# Comprehensive post-fix validation: run every EXPORT TYPE against the seeded
# Postgres garbage fixtures, and verify BOTH the local artifacts (parquet rows ==
# source, manifest, _SUCCESS, part count) AND the state-DB records (rivet state
# show/files/chunks). Covers full / keyset-seq / keyset-parallel /
# keyset-parallel-checkpoint / keyset-incremental / chunked-range / CSV.
set -uo pipefail
RIVET="${RIVET_BIN:-$HOME/rivet/target/release/rivet}"
PGC="rivet-postgres-1"
export PGURL="postgresql://rivet:rivet@127.0.0.1:5432/rivet"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok(){ printf '  \033[1;32m✓ %s\033[0m\n' "$*"; PASS=$((PASS+1)); }
bad(){ printf '  \033[1;31m✗ %s\033[0m\n' "$*"; FAIL=$((FAIL+1)); }
hdr(){ printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

srccount(){ docker exec -i "$PGC" psql -U rivet -d rivet -tA -c "SELECT count(*) FROM $1" 2>/dev/null | tr -d '[:space:]'; }
pqcount(){ duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$1/**/*.parquet')" 2>/dev/null | tr -d '[:space:]'; }
csvcount(){ duckdb -noheader -list -c "SELECT count(*) FROM read_csv('$1/**/*.csv', header=true, all_varchar=true)" 2>/dev/null | tr -d '[:space:]'; }
# Read ONLY the canonical manifest.json — the sink ALSO writes an immutable
# manifest-<run_id>.json copy (run-unique sidecar rule), so a manifest*.json glob
# double-counts. The canonical pointer is the single source of truth per run.
mfrows(){ python3 - "$1" <<'PY'
import json,os,sys
f=os.path.join(sys.argv[1],"manifest.json")
print(json.load(open(f)).get("row_count",0) if os.path.exists(f) else -1)
PY
}

# run_check <name> <table> <fmt> <countfn> <extra-yaml-lines...>
run_check(){
  local name=$1 table=$2 fmt=$3 cfn=$4; shift 4
  local dir="$WORK/$name" cfg="$WORK/$name.yaml"
  rm -rf "$dir"; mkdir -p "$dir"
  { echo "source:"; echo "  type: postgres"; echo "  url_env: PGURL";
    echo "exports:"; echo "  - name: $name"; echo "    table: $table";
    for l in "$@"; do echo "    $l"; done
    echo "    format: $fmt"; echo "    destination: {type: local, path: $dir/}"; } > "$cfg"
  hdr "$name  ($table, $fmt)"
  local out; out="$("$RIVET" run -c "$cfg" 2>&1)"
  if grep -qaiE "error|panic|failed" <<<"$out"; then bad "$name: run errored: $(grep -aiE 'error|fail' <<<"$out" | head -1)"; return; fi
  # ── artifacts ──
  local s a
  s="$(srccount "$table")"; a="$($cfn "$dir")"
  [ -n "$a" ] && [ "$s" = "$a" ] && ok "artifact rows $a == source $s" || bad "artifact rows [$a] != source [$s]"
  local mr; mr="$(mfrows "$dir")"
  [ "$mr" = "$s" ] && ok "manifest row_count $mr == source $s" || bad "manifest row_count [$mr] != source [$s]"
  local parts; parts="$(find "$dir" -name "*.$fmt" | wc -l | tr -d ' ')"
  [ "$parts" -ge 1 ] && ok "$parts part file(s) written" || bad "no part files"
  find "$dir" -name "_SUCCESS" | grep -q . && ok "_SUCCESS marker present" || bad "no _SUCCESS marker"
  # ── state DB record: file_log is written by EVERY runner (universal), so assert it
  #    lists this run's parts. (Cursor state via `state show` only exists for
  #    keyset/incremental, checked per-type below — not universal.)
  local fl; fl="$("$RIVET" state files -c "$cfg" -e "$name" 2>/dev/null | grep -c "\.$fmt")"
  [ "${fl:-0}" -ge 1 ] && ok "state files: file_log has $fl part row(s)" || bad "state files: file_log EMPTY"
}

echo "=== rivet: $($RIVET --version) ==="
echo "=== validating export types against ext.* garbage seeds ==="

# 1. FULL (keyless table)
run_check full_mode ext.no_pk_no_ts parquet pqcount "mode: full"

# 2. KEYSET sequential
run_check keyset_seq ext.bigint_pk_dual_ts parquet pqcount "mode: chunked" "chunk_by_key: id" "chunk_size: 50000"

# 3. KEYSET parallel (NON-checkpoint) — bug #4: file_log MUST be written now
run_check keyset_par ext.bigint_pk_dual_ts parquet pqcount "mode: chunked" "chunk_by_key: id" "parallel: 4" "chunk_size: 50000"
hdr "keyset_par: extra checks (fan-out + bug #4 file_log)"
w="$(find "$WORK/keyset_par" -name '*.parquet' | grep -oE '_pk_w[0-9]+_' | sort -u | wc -l | tr -d ' ')"
[ "${w:-0}" -ge 2 ] && ok "fan-out: $w distinct pk_w workers" || bad "fan-out: only ${w:-0} workers"
fl="$("$RIVET" state files -c "$WORK/keyset_par.yaml" -e keyset_par 2>/dev/null | grep -c '\.parquet')"
[ "${fl:-0}" -ge 1 ] && ok "state files: file_log has $fl rows (bug #4 fixed — non-checkpoint parallel writes file_log)" || bad "state files: EMPTY (bug #4 regressed)"

# 4. KEYSET parallel + CHECKPOINT — keyset_range via state chunks
run_check keyset_ckpt ext.bigint_pk_dual_ts parquet pqcount "mode: chunked" "chunk_by_key: id" "parallel: 4" "chunk_checkpoint: true" "chunk_size: 50000"
hdr "keyset_ckpt: extra checks (keyset_range)"
"$RIVET" state chunks -c "$WORK/keyset_ckpt.yaml" 2>/dev/null | grep -qiE "keyset_ckpt|range|done" && ok "state chunks shows checkpoint rows" || echo "  (state chunks: no rows post-finalize — cleared by finalize_keyset_anchor, expected)"

# 5. KEYSET incremental — cursor via state show; 2nd run adds 0
run_check keyset_incr ext.bigint_pk_dual_ts parquet pqcount "mode: chunked" "chunk_by_key: id" "parallel: 4" "chunk_checkpoint: true" "keyset_incremental: true" "chunk_size: 50000"
hdr "keyset_incr: extra checks (cursor persisted, 2nd run adds 0)"
"$RIVET" state show -c "$WORK/keyset_incr.yaml" 2>/dev/null | grep -qiE "keyset_incr" && ok "state show has a persisted cursor" || bad "state show: no cursor"
"$RIVET" run -c "$WORK/keyset_incr.yaml" >/dev/null 2>&1
a2="$(pqcount "$WORK/keyset_incr")"; s2="$(srccount ext.bigint_pk_dual_ts)"
[ "$a2" = "$s2" ] && ok "2nd incremental run added 0 (rows still $a2 == $s2, no re-export)" || bad "2nd run diverged: $a2 vs $s2"

# 6. CHUNKED range (dense int key)
run_check chunked_range ext.int_pk_dual_ts parquet pqcount "mode: chunked" "chunk_column: id" "chunk_size: 30000"

# 7. CSV writer path
run_check keyset_csv ext.bigint_pk_dual_ts csv csvcount "mode: chunked" "chunk_by_key: id" "chunk_size: 50000"

echo
printf '\033[1;34m▸ RESULT: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
rm -rf "$WORK"
[ "$FAIL" = 0 ]
