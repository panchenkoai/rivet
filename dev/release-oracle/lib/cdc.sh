# ── Release-oracle CDC end-to-end stage (sourced by scenarios.sh) ────────────
# CDC is the most engine-divergent, correctness-critical surface, and the batch
# scenarios (verdicts/integrity_types/keyset_parallel/load) never exercise it. This
# preflight codifies the manual CDC dogfood into the go/no-go gate: per engine, a
# typed table is anchored, mutated (INSERT/UPDATE/DELETE), captured (mode:cdc,
# until_current) to an object store, and INDEPENDENTLY read back by DuckDB (never
# rivet) — plus rivet's own `validate`, the state-DB metabase population, a
# SQLite-vs-Postgres state PARITY snapshot, and an at-least-once CRASH-recovery.
#
# Source-specific (a CDC-enabled engine per mechanism: PG logical slot, MySQL
# binlog, SQL Server change tables + Agent, Mongo change stream on a replica set),
# so — like verify_state_migrations — it runs ONCE from env URLs and SKIPs (never a
# silent pass) when they are absent. Set, per engine you want gated:
#   RIVET_CDC_POSTGRES_URL  postgresql://rivet:rivet@host:port/db   (wal_level=logical)
#   RIVET_CDC_MYSQL_URL     mysql://rivet:rivet@host:port/db        (log_bin, ROW)
#   RIVET_CDC_MSSQL_URL     sqlserver://rivet:rivet@host:port/db    (CDC enabled + Agent)
#   RIVET_CDC_MONGO_URL     mongodb://rivet:rivet@host:port/db?authSource=admin&directConnection=true
# and RIVET_CDC_STATE_URL (a Postgres db for the state-parity leg; SKIP that leg if unset).
#
# The reference state snapshot (эталонный слепок) is golden/cdc_state_snapshot.json:
# the canonical set of state-DB tables a CDC run populates. Both backends must match
# it — a release that stops populating run_status (or drifts the schema) fails here.

CDC_HOOK_FLUSH=cdc_after_flush_before_ack     # flushed to store, anchor NOT yet advanced
GOLDEN_CDC_SNAPSHOT="$HERE/golden/cdc_state_snapshot.json"

# The typed change set every engine applies: 3 INSERT, 1 UPDATE (id=2 → 55.55),
# 1 DELETE (id=3) → 5 change events; the id set {1,2,3} and the UPDATE after-image
# (55.55) are the completeness oracle. The crash leg then adds ONE row (id=4) and
# crashes mid-flush — a distinct id so the re-read proves at-least-once with no
# primary-key collision on the re-applied set.
CDC_EXPECT_EVENTS=5

# ── engine client shims (docker exec into whatever container serves the URL) ──
# The gate connects from the host; these run SQL against the CDC source. They locate
# the container by matching the URL's host:port to a published docker port, so the
# stage works against any CDC stand (repo-compose or versioned) without hard-coding.
_cdc_container_for() {  # $1 = url → prints the container serving its published port
  local port; port="$(printf '%s' "$1" | grep -oE ':[0-9]+/' | head -1 | tr -cd '0-9')"
  [ -z "$port" ] && return 1
  local c
  for c in $(docker ps --format '{{.Names}}'); do
    docker port "$c" 2>/dev/null | grep -q ":$port$" && { echo "$c"; return 0; }
  done
  return 1
}
# $1 = url; any further args pass through to psql (e.g. `-c "…"`). With no extra
# args psql reads SQL from stdin (heredoc callers). Both forms must work — a plain
# `docker exec … psql` that ignores "$@" silently drops a `-c` INSERT (the crash
# delta never ran, so at-least-once looked like a loss).
psql_cdc()   { local c; c="$(_cdc_container_for "$1")"; shift; docker exec -i "$c" psql -U rivet -d rivet -v ON_ERROR_STOP=1 -q "$@"; }
# SQL via STDIN, never `-e "$2"` — the changes carry JSON (`'{"k":1}'`) whose double
# quotes collide with a `-e "…"` argument and silently break the INSERT.
mysql_cdc()  { local c; c="$(_cdc_container_for "$1")"; printf '%s' "$2" | docker exec -i "$c" mysql -urivet -privet rivet 2>/dev/null; }
sqlcmd_cdc() { local c; c="$(_cdc_container_for "$1")"; if [ -n "${2:-}" ]; then docker exec -i "$c" /opt/mssql-tools18/bin/sqlcmd -S localhost -U rivet -P rivet -C -d rivet -b -Q "$2"; else docker exec -i "$c" /opt/mssql-tools18/bin/sqlcmd -S localhost -U rivet -P rivet -C -d rivet -b; fi; }
mongosh_cdc(){ local c; c="$(_cdc_container_for "$1")"; docker exec -i "$c" mongosh "mongodb://rivet:rivet@127.0.0.1:27017/rivet?authSource=admin" --quiet --eval "$2"; }

# ── per-engine: create+anchor the typed table / apply changes / crash-delta / clean ─
# Function names match the engine keys in the loop (postgres/mysql/mssql/mongo).
_cdc_postgres() { local url=$1 work=$2 slot; slot="orc_cdc_$(basename "$work" | tr 'A-Z.' 'a-z_' | tr -c 'a-z0-9_' '_')"
  echo "$slot" > "$work/slot"
  psql_cdc "$url" <<SQL
DROP TABLE IF EXISTS orc_cdc_probe;
SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name='$slot';
CREATE TABLE orc_cdc_probe (id int PRIMARY KEY, amount numeric(18,4), meta jsonb);
ALTER TABLE orc_cdc_probe REPLICA IDENTITY FULL;
SQL
  printf 'cdc: { slot: %s, until_current: true }\n' "$slot" > "$work/cdc.block"; }
_cdc_postgres_changes() { psql_cdc "$1" <<SQL
INSERT INTO orc_cdc_probe VALUES (1,12.3400,'{"k":1}'),(2,999999999999.9999,'{"k":2}'),(3,0.0001,'{"k":3}');
UPDATE orc_cdc_probe SET amount=55.5500 WHERE id=2;
DELETE FROM orc_cdc_probe WHERE id=3;
SQL
}
_cdc_postgres_crashdelta() { psql_cdc "$1" -c "INSERT INTO orc_cdc_probe VALUES (4,4.4400,'{\"k\":4}');"; }
_cdc_postgres_cleanup() { local url=$1 work=$2 slot; slot="$(cat "$work/slot" 2>/dev/null)"
  psql_cdc "$url" <<SQL >/dev/null 2>&1
SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name='$slot';
DROP TABLE IF EXISTS orc_cdc_probe;
SQL
}

_cdc_mysql() { local url=$1 work=$2
  mysql_cdc "$url" "DROP TABLE IF EXISTS orc_cdc_probe; CREATE TABLE orc_cdc_probe (id int PRIMARY KEY, amount decimal(18,4), meta json);"
  printf 'cdc: { until_current: true, checkpoint: "%s/cdc.ckpt", server_id: 7777 }\n' "$work" > "$work/cdc.block"; }
_cdc_mysql_changes() { mysql_cdc "$1" "INSERT INTO orc_cdc_probe VALUES (1,12.3400,'{\"k\":1}'),(2,999999999999.9999,'{\"k\":2}'),(3,0.0001,'{\"k\":3}'); UPDATE orc_cdc_probe SET amount=55.5500 WHERE id=2; DELETE FROM orc_cdc_probe WHERE id=3;"; }
_cdc_mysql_crashdelta() { mysql_cdc "$1" "INSERT INTO orc_cdc_probe VALUES (4,4.4400,'{\"k\":4}');"; }
_cdc_mysql_cleanup() { mysql_cdc "$1" "DROP TABLE IF EXISTS orc_cdc_probe;"; }

_cdc_mssql() { local url=$1 work=$2
  sqlcmd_cdc "$url" <<'SQL'
IF OBJECT_ID('dbo.orc_cdc_probe') IS NOT NULL DROP TABLE dbo.orc_cdc_probe;
CREATE TABLE dbo.orc_cdc_probe (id int PRIMARY KEY, amount decimal(18,4), meta nvarchar(200));
IF EXISTS (SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id=t.object_id WHERE t.name='orc_cdc_probe')
  EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='orc_cdc_probe', @capture_instance='all';
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orc_cdc_probe', @role_name=NULL, @capture_instance='dbo_orc_cdc_probe';
SQL
  printf 'cdc: { capture_instance: dbo_orc_cdc_probe, until_current: true, checkpoint: "%s/cdc.ckpt" }\n' "$work" > "$work/cdc.block"; }
_cdc_mssql_changes() { sqlcmd_cdc "$1" <<'SQL'
INSERT INTO dbo.orc_cdc_probe VALUES (1,12.3400,'{"k":1}'),(2,999999999999.9999,'{"k":2}'),(3,0.0001,'{"k":3}');
UPDATE dbo.orc_cdc_probe SET amount=55.5500 WHERE id=2;
DELETE FROM dbo.orc_cdc_probe WHERE id=3;
SQL
  sleep 8; }   # let the CDC capture job populate the change table
_cdc_mssql_crashdelta() { sqlcmd_cdc "$1" "INSERT INTO dbo.orc_cdc_probe VALUES (4,4.4400,'{\"k\":4}');"; sleep 8; }
_cdc_mssql_cleanup() { sqlcmd_cdc "$1" "IF OBJECT_ID('dbo.orc_cdc_probe') IS NOT NULL DROP TABLE dbo.orc_cdc_probe;" >/dev/null 2>&1; }

_cdc_mongo() { local url=$1 work=$2
  mongosh_cdc "$url" 'db.orc_cdc_probe.drop(); db.createCollection("orc_cdc_probe");'
  printf 'cdc: { until_current: true, checkpoint: "%s/cdc.ckpt" }\n' "$work" > "$work/cdc.block"; }
_cdc_mongo_changes() { mongosh_cdc "$1" 'db.orc_cdc_probe.insertMany([{_id:1,amount:12.34,meta:{k:1}},{_id:2,amount:999999999999.9999,meta:{k:2}},{_id:3,amount:0.0001,meta:{k:3}}]); db.orc_cdc_probe.updateOne({_id:2},{$set:{amount:55.55}}); db.orc_cdc_probe.deleteOne({_id:3});'; }
_cdc_mongo_crashdelta() { mongosh_cdc "$1" 'db.orc_cdc_probe.insertOne({_id:4,amount:4.44,meta:{k:4}});'; }
_cdc_mongo_cleanup() { mongosh_cdc "$1" 'db.orc_cdc_probe.drop();' >/dev/null 2>&1; }

# ── write a CDC config to $work/cdc.yaml pointing at $store, using the engine block ──
_cdc_write_cfg() {  # $1=eng $2=url $3=work $4=store
  local eng=$1 url=$2 work=$3 store=$4 bkt pfx dest tls=""
  bkt="$(cfg store "$store" bucket)"; pfx="oracle-cdc/${WORK##*/}/${eng}/${store}/$(basename "$work")"
  echo "$pfx" > "$work/pfx"; echo "$bkt" > "$work/bkt"
  dest="$(_store_dest "$store" "$bkt" "$pfx")" || return 1
  [ "$eng" = mssql ] && tls=$'\n  tls: { accept_invalid_certs: true }'
  cat > "$work/cdc.yaml" <<YAML
source:
  type: $eng
  url: "$url"$tls
exports:
  - name: orc_cdc_probe
    table: orc_cdc_probe
    mode: cdc
    format: parquet
    $(cat "$work/cdc.block")
    destination:
$dest
YAML
}

# INDEPENDENT distinct id-set the store holds (never rivet) — completeness oracle.
# $4 = id column (id for SQL, _id for mongo — parquet can't COALESCE an absent column).
_cdc_store_ids() { local store=$1 bkt=$2 pfx=$3 idc=$4
  case "$store" in
    s3) duckdb -noheader -list -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='127.0.0.1:9000'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; SELECT string_agg(DISTINCT CAST($idc AS VARCHAR),',') FROM read_parquet('s3://$bkt/$pfx/**/*.parquet')" 2>/dev/null;;
  esac
}

# ── the CDC end-to-end preflight (once, env-driven, SKIP-if-absent) ──────────
verify_cdc_e2e() {
  local any=0
  export MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin
  export AZURITE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  local eng
  for eng in postgres mysql mssql mongo; do
    local uvar; uvar="RIVET_CDC_$(printf '%s' "$eng" | tr a-z A-Z)_URL"
    local url="${!uvar:-}"
    [ -z "$url" ] && { skip "cdc[$eng]: no $uvar"; add "$eng" cdc e2e - SKIP "no url"; continue; }
    any=1
    log "CDC end-to-end [$eng] (anchor → typed changes → capture → store → independent readback + validate + state + crash)"
    local work; work="$(mktemp -d)"
    local idc; idc="$([ "$eng" = mongo ] && echo _id || echo id)"
    if ! "_cdc_${eng}" "$url" "$work"; then bad "cdc[$eng]: source setup failed"; add "$eng" cdc e2e - FAIL setup; continue; fi
    _cdc_write_cfg "$eng" "$url" "$work" s3 || { skip "cdc[$eng]: no s3 dest"; add "$eng" cdc e2e s3 SKIP "no dest"; continue; }
    # 1. clean capture: anchor (A) → changes → capture (B)
    "$RIVET" run -c "$work/cdc.yaml" >/dev/null 2>&1
    "_cdc_${eng}_changes" "$url" >/dev/null 2>&1
    "$RIVET" run -c "$work/cdc.yaml" >/dev/null 2>&1
    local n; n="$(_store_readback s3 "$(cat "$work/bkt")" "$(cat "$work/pfx")")"          # INDEPENDENT (DuckDB)
    local vout; vout="$("$RIVET" validate -c "$work/cdc.yaml" 2>&1)"; local vp=0; grep -qai "PASSED" <<<"$vout" && vp=1
    local sdb="$work/.rivet_state.db" spop=0
    [ -f "$sdb" ] && [ "$(sqlite3 "$sdb" "SELECT CASE WHEN count(*)>0 AND count(*)=sum(status='success') THEN 1 ELSE 0 END FROM run_status" 2>/dev/null)" = 1 ] && spop=1
    # 2. crash-recovery: an extra row (id=4) + crash at cdc_after_flush_before_ack →
    #    anchor held → recovery re-reads it (at-least-once, no loss).
    "_cdc_${eng}_crashdelta" "$url" >/dev/null 2>&1
    RIVET_TEST_PANIC_AT="$CDC_HOOK_FLUSH" "$RIVET" run -c "$work/cdc.yaml" >/dev/null 2>&1   # crash
    "$RIVET" run -c "$work/cdc.yaml" >/dev/null 2>&1                                          # recover
    local ids; ids="$(_cdc_store_ids s3 "$(cat "$work/bkt")" "$(cat "$work/pfx")" "$idc")"
    local crashok=0; echo "$ids" | grep -q 4 && crashok=1
    "_cdc_${eng}_cleanup" "$url" "$work" >/dev/null 2>&1

    local fails=""
    [ "${n:-0}" = "$CDC_EXPECT_EVENTS" ] || fails+="independent-readback[$n!=$CDC_EXPECT_EVENTS] "
    [ "$vp" = 1 ] || fails+="validate-not-PASSED "
    [ "$spop" = 1 ] || fails+="state-not-populated "
    [ "$crashok" = 1 ] || fails+="crash-lost-the-delta[ids=$ids] "
    if [ -z "$fails" ]; then ok "cdc[$eng]: readback=$n, validate PASSED, state populated, crash at-least-once (id=4 re-read)"; add "$eng" cdc e2e s3 PASS "$n events"
    else bad "cdc[$eng]: $fails"; add "$eng" cdc e2e s3 FAIL "$fails"; fi
  done
  [ "$any" = 0 ] && return
  _cdc_large_tx_atomic
  _cdc_state_parity
}

# ── large-transaction atomicity (the committed-boundary invariant) ───────────
# A source transaction LARGER than `rollover` must roll as ONE unit — the adapter
# marks ONLY its last event `committed`, so the sink flushes+checkpoints+acks at the
# true commit boundary, never mid-transaction. A crash mid-flush therefore holds the
# anchor BEFORE the whole transaction; recovery re-reads it entire, losing no tail.
# RED-proven against the `committed:true`-on-every-event mutant (which rolls + acks
# mid-transaction, so a crash advances the anchor past the committed prefix and the
# tail is skipped on resume). PG only — the slot-anchor engine where the mid-transaction
# advance is observable (the sink code is shared, so one engine proves the invariant).
_cdc_large_tx_atomic() {
  local url="${RIVET_CDC_POSTGRES_URL:-}"
  [ -z "$url" ] && { skip "cdc large-tx-atomic: no RIVET_CDC_POSTGRES_URL"; add postgres cdc large-tx-atomic - SKIP "no pg cdc url"; return; }
  export MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin
  log "CDC large-transaction atomicity (a >rollover transaction survives a mid-flush crash whole)"
  local work; work="$(mktemp -d)"
  local slot; slot="orc_ltx_$(basename "$work" | tr 'A-Z.' 'a-z_' | tr -c 'a-z0-9_' '_')"
  local bkt pfx; bkt="$(cfg store s3 bucket)"; pfx="oracle-cdc-ltx/${WORK##*/}/$(basename "$work")"
  psql_cdc "$url" <<SQL
DROP TABLE IF EXISTS orc_ltx;
SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name='$slot';
CREATE TABLE orc_ltx (id int PRIMARY KEY);
ALTER TABLE orc_ltx REPLICA IDENTITY FULL;
SQL
  cat > "$work/ltx.yaml" <<YAML
source: { type: postgres, url: "$url" }
exports:
  - name: orc_ltx
    table: orc_ltx
    mode: cdc
    format: parquet
    cdc: { slot: $slot, until_current: true, rollover: 5 }
    destination:
$(_store_dest s3 "$bkt" "$pfx")
YAML
  "$RIVET" run -c "$work/ltx.yaml" >/dev/null 2>&1                      # anchor
  # ONE transaction of 12 rows — larger than rollover 5. Must NOT split.
  psql_cdc "$url" <<SQL
BEGIN;
INSERT INTO orc_ltx SELECT g FROM generate_series(1,12) g;
COMMIT;
SQL
  RIVET_TEST_PANIC_AT=cdc_after_ack "$RIVET" run -c "$work/ltx.yaml" >/dev/null 2>&1   # crash mid-flush
  "$RIVET" run -c "$work/ltx.yaml" >/dev/null 2>&1                                      # recover
  local ids cnt; ids="$(_cdc_store_ids s3 "$bkt" "$pfx" id)"
  cnt="$(printf '%s' "$ids" | tr ',' '\n' | grep -cE '^[0-9]+$')"
  psql_cdc "$url" -c "SELECT pg_drop_replication_slot('$slot') FROM pg_replication_slots WHERE slot_name='$slot'; DROP TABLE IF EXISTS orc_ltx;" >/dev/null 2>&1
  if [ "${cnt:-0}" -eq 12 ]; then ok "cdc large-tx-atomic: all 12 rows of the >rollover transaction survived the mid-flush crash"; add postgres cdc large-tx-atomic s3 PASS "12/12"
  else bad "cdc large-tx-atomic: only $cnt/12 rows survived — the transaction split + lost its tail across the crash"; add postgres cdc large-tx-atomic s3 FAIL "$cnt/12 (ids=$ids)"; fi
}

# ── SQLite-vs-Postgres state PARITY + reference snapshot (эталонный слепок) ──
# A canonical PG CDC run against BOTH state backends; the populated table set must
# match golden/cdc_state_snapshot.json on each. RIVET_CDC_STATE_URL provides the PG
# state db; SKIP the PG leg (not the SQLite one) when unset.
_cdc_state_parity() {
  local url="${RIVET_CDC_POSTGRES_URL:-}"
  [ -z "$url" ] && { skip "cdc state-parity: no RIVET_CDC_POSTGRES_URL"; add postgres cdc state-parity - SKIP "no pg cdc url"; return; }
  # Self-contained store creds — this runs standalone under --bless-cdc, not only
  # after verify_cdc_e2e's export; without them the s3 write fails and every run's
  # status is non-success, poisoning the reference snapshot (run_status_all_success).
  export MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin
  log "CDC state metabase parity (SQLite vs Postgres) + reference snapshot"
  local w1; w1="$(mktemp -d)"; _cdc_postgres "$url" "$w1"; _cdc_write_cfg postgres "$url" "$w1" s3
  "$RIVET" run -c "$w1/cdc.yaml" >/dev/null 2>&1; _cdc_postgres_changes "$url" >/dev/null 2>&1; "$RIVET" run -c "$w1/cdc.yaml" >/dev/null 2>&1
  local snap_sqlite; snap_sqlite="$(_cdc_state_snapshot_sqlite "$w1/.rivet_state.db")"; _cdc_postgres_cleanup "$url" "$w1"

  local want; want="$(python3 -c "import json,os;print(json.dumps(json.load(open('$GOLDEN_CDC_SNAPSHOT')),sort_keys=True) if os.path.exists('$GOLDEN_CDC_SNAPSHOT') else '')" 2>/dev/null)"
  local got_sqlite; got_sqlite="$(python3 -c "import json,sys;print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$snap_sqlite" 2>/dev/null)"

  local surl="${RIVET_CDC_STATE_URL:-}" snap_pg="" got_pg=""
  if [ -n "$surl" ]; then
    # The SQLite leg is fresh per $work; the Postgres state db is PERSISTENT and
    # accumulates across runs (run_aggregate/run_journal grow), so it would diverge
    # from the fresh-run golden. Reset it so the snapshot reflects a SINGLE run —
    # parity is "same table set populated", not "same accumulated history".
    _cdc_pg_state_reset "$surl"
    local w2; w2="$(mktemp -d)"; _cdc_postgres "$url" "$w2"; _cdc_write_cfg postgres "$url" "$w2" s3
    RIVET_STATE_URL="$surl" "$RIVET" run -c "$w2/cdc.yaml" >/dev/null 2>&1; _cdc_postgres_changes "$url" >/dev/null 2>&1
    RIVET_STATE_URL="$surl" "$RIVET" run -c "$w2/cdc.yaml" >/dev/null 2>&1
    snap_pg="$(_cdc_state_snapshot_pg "$surl")"; _cdc_postgres_cleanup "$url" "$w2"
    got_pg="$(python3 -c "import json,sys;print(json.dumps(json.loads(sys.argv[1]),sort_keys=True))" "$snap_pg" 2>/dev/null)"
  fi

  if [ "${BLESS_CDC:-0}" = 1 ]; then
    mkdir -p "$HERE/golden"; printf '%s\n' "$snap_sqlite" | python3 -c "import json,sys;json.dump(json.load(sys.stdin),open('$GOLDEN_CDC_SNAPSHOT','w'),indent=2,sort_keys=True)"
    ok "blessed cdc state snapshot"; add postgres cdc state-parity - PASS blessed; return
  fi
  local fails=""
  [ -n "$want" ] || fails+="no-golden(bless first) "
  [ "$got_sqlite" = "$want" ] || fails+="sqlite!=golden "
  [ -n "$surl" ] && { [ "$got_pg" = "$want" ] || fails+="postgres!=golden "; }
  if [ -z "$fails" ]; then ok "cdc state parity: SQLite$([ -n "$surl" ] && echo ' == Postgres') == golden snapshot"; add postgres cdc state-parity - PASS "$([ -n "$surl" ] && echo sqlite+pg || echo sqlite-only)"
  else bad "cdc state parity: $fails"; add postgres cdc state-parity - FAIL "$fails"; fi
}

# reference snapshot: the table→populated? map a CDC run leaves + the all-success flag.
_cdc_state_snapshot_sqlite() { python3 - "$1" <<'PY'
import sqlite3,sys,json
c=sqlite3.connect(sys.argv[1]); snap={}
for t in "run_status export_metrics run_journal run_aggregate export_state export_schema".split():
    try: n=c.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    except Exception: n=0
    snap[t]="populated" if n>0 else "empty"
try: n,s=c.execute("SELECT count(*), sum(status='success') FROM run_status").fetchone(); snap["run_status_all_success"]="yes" if n and n==s else "no"
except Exception: snap["run_status_all_success"]="no"
print(json.dumps(snap,sort_keys=True))
PY
}
# Reset the PG state db to empty so a parity snapshot reflects one run, not the
# accumulated history (the SQLite leg is a fresh file per run).
_cdc_pg_state_reset() {  # $1 = state url
  local c sdb; c="$(_cdc_container_for "$1")"; sdb="$(printf '%s' "$1" | sed -E 's#.*/([^/?]+).*#\1#')"
  docker exec -i "$c" psql -U rivet -d "$sdb" -q -c "DROP TABLE IF EXISTS run_status,export_metrics,run_journal,run_aggregate,export_state,export_schema,schema_version,rivet_schema_version,cdc_snapshot,file_log,chunk_run,chunk_task,keyset_range,export_progression,export_shape,export_harm,load_run,loaded_source_run CASCADE;" >/dev/null 2>&1
}
_cdc_state_snapshot_pg() {  # $1 = RIVET_CDC_STATE_URL — read via the source container's psql
  local c sdb; c="$(_cdc_container_for "${RIVET_CDC_POSTGRES_URL}")"; sdb="$(printf '%s' "$1" | sed -E 's#.*/([^/?]+).*#\1#')"
  python3 - "$c" "$sdb" <<'PY'
import subprocess,sys,json
c,db=sys.argv[1],sys.argv[2]; snap={}
def q(sql):
    return subprocess.run(["docker","exec","-i",c,"psql","-U","rivet","-d",db,"-tAc",sql],capture_output=True,text=True).stdout.strip()
for t in "run_status export_metrics run_journal run_aggregate export_state export_schema".split():
    n=q(f"SELECT count(*) FROM {t}") or "0"
    snap[t]="populated" if n.isdigit() and int(n)>0 else "empty"
r=q("SELECT count(*)||' '||COALESCE(sum((status='success')::int),0) FROM run_status").split()
snap["run_status_all_success"]="yes" if len(r)==2 and r[0]!="0" and r[0]==r[1] else "no"
print(json.dumps(snap,sort_keys=True))
PY
}
