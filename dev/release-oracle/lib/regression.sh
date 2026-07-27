# ── Release-oracle: regression vs the PREVIOUS RELEASE (sourced by scenarios.sh) ──
# The gate compares to checked-in goldens and to itself, never to the version users
# are ACTUALLY running. Two regressions invisible to every correctness check yet
# release-blocking:
#   B-format: the new release must READ what the previous release WROTE (manifest +
#             parts). A format bump that can't read old artifacts silently breaks every
#             existing user's data on upgrade — worse than a crash, it's a quiet loss.
#   B-perf:   a 3x slowdown or an RSS blow-up ships GREEN through every count/value
#             check. Benchmarked against the DOWNLOADED previous-release binary (the
#             artifact users run — a GitHub release asset / brew bottle), NEVER a
#             rebuilt parent (the release profile is fat-LTO; a rebuild is minutes AND
#             a locally-rebuilt approximation, not what shipped).
#
# prev = RIVET_PREV_RELEASE_BIN (download it: gh release download vX.Y.Z -p '*<target>*'),
# source = RIVET_REGRESSION_SOURCE_URL (a postgres the gate can seed). SKIP (never a
# silent pass) when either is absent. Wall tolerance RIVET_REGRESSION_WALL_TOL (def 1.5×
# — a go/no-go catches gross regressions, fine-grained perf is the benchmark suite's job).
#
# CROSS-VERSION STATE: each binary gets its OWN env dir, so its own `.rivet_state.db`
# (which lives next to the config). The new binary UPGRADES the state schema (v18→v19);
# the old binary then cannot open it ("migration incomplete"). Never share a state dir.

_regr_psql() { local url=$1; local c port; port="$(printf '%s' "$url" | grep -oE ':[0-9]+/' | head -1 | tr -cd '0-9')"
  for c in $(docker ps --format '{{.Names}}'); do docker port "$c" 2>/dev/null | grep -q ":$port$" && { docker exec -i "$c" psql -U rivet -d rivet -v ON_ERROR_STOP=1 -q; return; }; done; return 1; }

# run "$@", echo "wall_seconds rss_bytes". Detect BSD (-l, macOS) vs GNU (-v) time by
# probing `-l` on a trivial command — NEVER by the timed command's exit code (a run
# that returns non-zero would else misfire the fallback and lose the timing).
_regr_time() { local t; t="$(mktemp)"
  if /usr/bin/time -l true >/dev/null 2>&1; then /usr/bin/time -l "$@" >/dev/null 2>"$t"   # BSD/macOS
  else /usr/bin/time -v "$@" >/dev/null 2>"$t"; fi                                          # GNU
  local wall rss
  wall="$(grep -aoE '[0-9]+\.[0-9]+ real' "$t" | grep -aoE '[0-9]+\.[0-9]+' | head -1)"
  [ -z "$wall" ] && wall="$(grep -aoE 'wall clock.*[0-9]+:[0-9.]+' "$t" | grep -aoE '[0-9]+\.[0-9]+' | tail -1)"
  rss="$(grep -aoE '[0-9]+ +maximum resident set size' "$t" | grep -aoE '^[0-9]+' | head -1)"
  [ -z "$rss" ] && rss="$(( $(grep -aoE 'Maximum resident set size.*[0-9]+' "$t" | grep -aoE '[0-9]+' | tail -1 || echo 0) * 1024 ))"
  rm -f "$t"; echo "${wall:-0} ${rss:-0}"; }

# write an isolated env: a keyset+zstd export config at $1/c.yaml, state + output in $1.
# $2 (optional) overrides the destination dir (to read ANOTHER binary's output).
_regr_cfg() { mkdir -p "$1/out"; cat > "$1/c.yaml" <<YAML
source: { type: postgres, url: "$REGR_SRC" }
exports:
  - name: regr_probe
    table: regr_probe
    mode: chunked
    chunk_by_key: id
    chunk_size: 50000
    format: parquet
    compression: zstd
    destination: { type: local, path: "${2:-$1/out}/" }
YAML
}

verify_release_regression() {
  local prev="${RIVET_PREV_RELEASE_BIN:-}"
  { [ -n "$prev" ] && [ -x "$prev" ]; } || { skip "release regression: no RIVET_PREV_RELEASE_BIN (download a release asset)"; add release regression - SKIP "no prev binary"; return; }
  export REGR_SRC="${RIVET_REGRESSION_SOURCE_URL:-}"
  [ -z "$REGR_SRC" ] && { skip "release regression: no RIVET_REGRESSION_SOURCE_URL"; add release regression - SKIP "no source"; return; }
  local tol="${RIVET_REGRESSION_WALL_TOL:-1.5}"
  log "Release regression vs prev ($("$prev" --version 2>/dev/null|head -1)) — cross-version read + perf/RSS"
  local work; work="$(mktemp -d)"

  # deterministic 100K-row fixture (measurable, sub-second) — seeded, not data-dependent.
  _regr_psql "$REGR_SRC" <<SQL
DROP TABLE IF EXISTS regr_probe;
CREATE TABLE regr_probe (id int PRIMARY KEY, a text, b numeric(18,4), c timestamptz);
INSERT INTO regr_probe SELECT g, md5(g::text), (g%1000)+0.25, '2025-01-01'::timestamptz + (g||' seconds')::interval FROM generate_series(1,100000) g;
SQL

  local fails=""
  # ── B-format: prev WRITES → cur READS its manifest+parts (forward compat, upgrade path) ──
  local pe="$work/prev_fmt"; _regr_cfg "$pe"
  "$prev" run -c "$pe/c.yaml" >/dev/null 2>&1 || fails+="prev-export-failed "
  # cur validate re-reads prev's OUTPUT (pe/out) from cur's OWN env (ce) — so cur's state
  # upgrade never touches prev's state db.
  local ce="$work/cur_fmt"; _regr_cfg "$ce" "$pe/out"
  grep -qai "PASSED" <<<"$("$RIVET" validate -c "$ce/c.yaml" 2>&1)" || fails+="cur-cannot-read-prev-output(format-break) "
  # independent cross-check: DuckDB reads prev's parts, row count == source.
  local dcnt scnt; dcnt="$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$pe/out/**/*.parquet')" 2>/dev/null)"
  scnt="$(_regr_psql "$REGR_SRC" <<<'SELECT count(*) FROM regr_probe;' 2>/dev/null | grep -aoE '[0-9]+' | head -1)"
  { [ -n "$dcnt" ] && [ "$dcnt" = "$scnt" ]; } || fails+="prev-parts-rowcount[$dcnt!=$scnt] "

  # ── B-perf: cur vs the DOWNLOADED prev release, each in its OWN env (own state db).
  #    One warm pass (page in the binary + prime cache), then a timed pass into a fresh
  #    output dir so the measurement is a single clean run. ──
  local pp="$work/perf_prev" pc="$work/perf_cur"; _regr_cfg "$pp"; _regr_cfg "$pc"
  "$prev" run -c "$pp/c.yaml" >/dev/null 2>&1; "$RIVET" run -c "$pc/c.yaml" >/dev/null 2>&1   # warm
  rm -rf "$pp/out" "$pc/out"; mkdir -p "$pp/out" "$pc/out"
  local pw pr cw cr; read -r pw pr < <(_regr_time "$prev" run -c "$pp/c.yaml"); read -r cw cr < <(_regr_time "$RIVET" run -c "$pc/c.yaml")
  awk -v c="$cw" -v p="$pw" -v t="$tol" 'BEGIN{exit !(c>0 && p>0 && c <= p*t)}' \
    || fails+="perf-regression(cur ${cw}s > prev ${pw}s ×${tol}) "

  _regr_psql "$REGR_SRC" <<<'DROP TABLE IF EXISTS regr_probe;' >/dev/null 2>&1
  local rssnote=""; { [ "${cr:-0}" -gt 0 ] && [ "${pr:-0}" -gt 0 ]; } && rssnote=" RSS cur $((cr/1048576))MB vs prev $((pr/1048576))MB"
  if [ -z "$fails" ]; then ok "release regression: cur reads prev's output (format-compat), perf cur ${cw}s <= prev ${pw}s ×${tol}${rssnote}"; add release regression - PASS "wall ${cw}/${pw}s"
  else bad "release regression: $fails"; add release regression - FAIL "$fails"; fi
}
