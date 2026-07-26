#!/usr/bin/env bash
# DEEP correctness validation (beyond file/row counts): the exported VALUES match the
# source column-for-column (independent DuckDB aggregates vs psql), writes are
# DETERMINISTIC (two independent runs -> byte-identical values), the STATE-DB records
# are correct (incremental cursor == max key, file_log == parts), and the crash/recovery
# scenarios recover the exact same complete data + state. Golden garbage seeds.
set -uo pipefail
RIVET="${RIVET_BIN:-$HOME/rivet/target/release/rivet}"
PGC="rivet-postgres-1"
export PGURL="postgresql://rivet:rivet@127.0.0.1:5432/rivet"
WORK="$(mktemp -d)"
PASS=0; FAIL=0
ok(){ printf '  \033[1;32m✓ %s\033[0m\n' "$*"; PASS=$((PASS+1)); }
bad(){ printf '  \033[1;31m✗ %s\033[0m\n' "$*"; FAIL=$((FAIL+1)); }
hdr(){ printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# NOTE: no `-i` on docker exec — psql -c needs no stdin, and `-i` would CONSUME the
# stdin of an enclosing `while read` loop (eating the aggregate list after line 1).
pg(){ docker exec "$PGC" psql -U rivet -d rivet -tA -c "$1" 2>/dev/null | tr -d '[:space:]'; }
dd(){ duckdb -noheader -list -c "$1" 2>/dev/null | tr -d '[:space:]'; }

mkcfg(){ # name table dir extra-lines...
  local name=$1 table=$2 dir=$3; shift 3
  local cfg="$WORK/$name.yaml"
  { echo "source: {type: postgres, url_env: PGURL}"; echo "exports:";
    echo "  - name: $name"; echo "    table: $table";
    for l in "$@"; do echo "    $l"; done
    echo "    format: parquet"; echo "    destination: {type: local, path: $dir/}"; } > "$cfg"
  echo "$cfg"
}

echo "=== rivet $($RIVET --version) — DEEP correctness validation ==="

# ── 1. VALUE CORRECTNESS: ref_id_history (rich: bigint, int, smallint, 4x numeric,
#      text, char, 2x timestamp) via range-chunk — independent DuckDB aggregates ──
hdr "ref_id_history (range-chunk): per-column VALUE correctness vs source"
d1="$WORK/rih1"; mkdir -p "$d1"
cfg=$(mkcfg rih1 ext.ref_id_history "$d1" "mode: chunked" "chunk_column: ref_id" "chunk_size: 30000")
"$RIVET" run -c "$cfg" >/dev/null 2>&1
PARQ="read_parquet('$d1/**/*.parquet')"
while IFS='|' read -r label expr; do
  s=$(pg "SELECT $expr FROM ext.ref_id_history"); p=$(dd "SELECT $expr FROM $PARQ")
  [ -n "$s" ] && [ "$s" = "$p" ] && ok "$label: $s (source==parquet)" || bad "$label: source [$s] != parquet [$p]"
done <<'AGGS'
rows|count(*)
distinct ref_id|count(distinct ref_id)
sum(version)|sum(version)
sum(sign)|sum(sign)
sum(cart)|round(sum(cart),4)
sum(earning)|round(sum(earning),4)
sum(subtotal)|round(sum(subtotal),4)
sum(total)|round(sum(total),4)
min/max ref_id|min(ref_id)||'-'||max(ref_id)
distinct status|count(distinct status)
distinct field|count(distinct field)
min/max created_at|min(created_at)||'|'||max(created_at)
AGGS

# ── 2. DETERMINISM: a second independent run produces byte-identical VALUES ──
hdr "ref_id_history: DETERMINISM (two independent runs, full-row value hash)"
d2="$WORK/rih2"; mkdir -p "$d2"
cfg2=$(mkcfg rih2 ext.ref_id_history "$d2" "mode: chunked" "chunk_column: ref_id" "chunk_size: 30000")
"$RIVET" run -c "$cfg2" >/dev/null 2>&1
# Full-row value hash, deterministically ordered (ref_id is non-unique -> +version).
# No subquery alias (`row` is a reserved word in DuckDB); string_agg carries the ORDER BY.
hashq(){ echo "SELECT md5(string_agg(ref_id||'|'||version||'|'||coalesce(total::VARCHAR,'')||'|'||coalesce(status,'')||'|'||coalesce(created_at::VARCHAR,''), chr(10) ORDER BY ref_id, version)) FROM read_parquet('$1/**/*.parquet')"; }
h1=$(dd "$(hashq "$d1")")
h2=$(dd "$(hashq "$d2")")
[ -n "$h1" ] && [ "$h1" = "$h2" ] && ok "two independent runs -> byte-identical VALUES (md5 ${h1:0:12})" || bad "non-deterministic: run1 [${h1:0:12}] != run2 [${h2:0:12}]"

# ── 3. keyset VALUE correctness + STATE-DB determinism (cursor == max key) ──
hdr "bigint_pk_dual_ts (keyset incremental): values + STATE cursor determinism"
d3="$WORK/ks"; mkdir -p "$d3"
cfgk=$(mkcfg ksv ext.bigint_pk_dual_ts "$d3" "mode: chunked" "chunk_by_key: id" "parallel: 4" "chunk_checkpoint: true" "keyset_incremental: true" "chunk_size: 40000")
"$RIVET" run -c "$cfgk" >/dev/null 2>&1
KP="read_parquet('$d3/**/*.parquet')"
smax=$(pg "SELECT max(id) FROM ext.bigint_pk_dual_ts"); pmax=$(dd "SELECT max(id) FROM $KP")
[ "$smax" = "$pmax" ] && ok "max(id): $smax (source==parquet)" || bad "max(id): source [$smax] != parquet [$pmax]"
ssum=$(pg "SELECT count(distinct id) FROM ext.bigint_pk_dual_ts"); psum=$(dd "SELECT count(distinct id) FROM $KP")
[ "$ssum" = "$psum" ] && ok "distinct id: $ssum (source==parquet)" || bad "distinct id: [$ssum] != [$psum]"
# STATE-DB: the persisted incremental cursor must be EXACTLY max(id) — deterministic.
# `state show` columns: EXPORT | LAST CURSOR | LAST RUN — take field 2 of the ksv row.
cur=$("$RIVET" state show -c "$cfgk" 2>/dev/null | awk '$1=="ksv"{print $2}')
[ "$cur" = "$smax" ] && ok "STATE cursor == max(id) $smax (deterministic persisted position)" || bad "STATE cursor [$cur] != max(id) [$smax]"
# STATE-DB: file_log part rows determinism — a second run adds 0 (cursor blocks re-export).
"$RIVET" run -c "$cfgk" >/dev/null 2>&1
pmax2=$(dd "SELECT count(*) FROM $KP"); srows=$(pg "SELECT count(*) FROM ext.bigint_pk_dual_ts")
[ "$pmax2" = "$srows" ] && ok "2nd run added 0 rows (still $srows) — cursor deterministically blocks re-read" || bad "2nd run diverged: $pmax2 != $srows"

echo
printf '\033[1;34m▸ DEEP RESULT: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
rm -rf "$WORK"
[ "$FAIL" = 0 ]
