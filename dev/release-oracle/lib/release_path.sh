# ── Release-oracle: the RELEASE BUILD/PUBLISH path stage (sourced by scenarios.sh) ─
# The batch/CDC scenarios prove CORRECTNESS of what ships; they assume a working
# binary already exists. But the release pipeline runs DIFFERENT, stricter tooling
# than `cargo build`, and the gap only surfaces at the TAG — after crates.io, the
# binaries, and the GitHub release have published, when the failure is no longer
# re-runnable from the immutable tag:
#   - 0.16.0 shipped broken Docker images: the Dockerfile's `cargo chef prepare`
#     parses Cargo.toml with `cargo-manifest` (spec-strict), which rejects the
#     multi-line inline tables `cargo build` tolerates. Green everywhere else.
#   - 0.16.1: `cargo publish --locked` aborted on a Cargo.lock not committed in sync
#     with the bumped Cargo.toml — again AFTER the tag was cut.
# CI guards both now, but the go/no-go GATE (this oracle) did not — so a pre-tag
# `run.sh` could be green while the release build path is broken. This runs the real
# path (or its exact sub-steps), so the mismatch fails LOUD and pre-tag.
#
# MUST run FIRST, before any other cargo command reconciles the working-tree lock:
# `cargo metadata --locked` only catches a stale committed lock while the lock is
# still stale — a `cargo build`/`test` silently fixes it before the check would see it.

verify_release_build_path() {
  command -v cargo >/dev/null 2>&1 || { skip "release build path: cargo absent"; add release build_path - SKIP "no cargo"; return; }
  log "Release build path (lock sync / manifest-chef parse / schema regen / cargo-chef planner / [docker image])"
  local fails="" note=""

  # 1. Cargo.lock in sync with Cargo.toml — the release publishes with `cargo publish
  #    --locked` and the Docker builder cooks + builds `--locked` (0.16.1). Run this
  #    FIRST: `cargo metadata --locked` resolves without compiling (~seconds) AND
  #    before anything reconciles the lock out from under it.
  ( cd "$ROOT" && cargo metadata --locked --format-version 1 >/dev/null 2>&1 ) \
    || fails+="lock-out-of-sync(publish/docker --locked would abort) "

  # 2. The offline guards that mirror the release-only tooling: the cargo-manifest
  #    strict parse (0.16.0 — no multi-line inline tables in Cargo.toml) and the
  #    JSON-schema regen (a version bump that forgot `rivet schema config`).
  if ! ( cd "$ROOT" && cargo test --test offline_suite cargo_manifest_chef >/dev/null 2>&1 \
         && cargo test --test offline_suite schema_drift >/dev/null 2>&1 ); then
    fails+="offline-release-guard(manifest-chef/schema-drift) "
  fi

  # 3. The ACTUAL first Docker step — `cargo chef prepare` (Dockerfile stage `planner`,
  #    line 11). It distils the dep graph with NO compilation, so it is cheap yet runs
  #    the exact cargo-manifest parse the 0.16.0 image failed on — a proxy that IS the
  #    real command, not a stand-in. SKIP (noted) when cargo-chef is not installed.
  if ( cd "$ROOT" && cargo chef --version >/dev/null 2>&1 ); then
    ( cd "$ROOT" && cargo chef prepare --recipe-path "$WORK/recipe.json" >/dev/null 2>&1 ) \
      || fails+="cargo-chef-prepare(the Docker planner step) "
  else
    note="cargo-chef absent (docker planner step not run) "
  fi

  # 4. The full release Docker image — the complete real path (0.16.0 was ONLY caught
  #    here). Minutes (fat-LTO in-image), so opt-in via RIVET_ORACLE_DOCKER=1; the
  #    steps above cover the known regression classes cheaply on every run.
  if [ "${RIVET_ORACLE_DOCKER:-0}" = 1 ]; then
    if command -v docker >/dev/null 2>&1; then
      ( cd "$ROOT" && docker build -f Dockerfile -t rivet-oracle-release-check . >"$WORK/docker_build.log" 2>&1 ) \
        || fails+="docker-image-build(see $WORK/docker_build.log) "
    else
      note+="docker absent (image build skipped) "
    fi
  else
    note+="full docker build skipped (set RIVET_ORACLE_DOCKER=1) "
  fi

  if [ -z "$fails" ]; then
    ok "release build path: lock in sync, manifest-chef + schema guards pass, cargo-chef planner ok${note:+ (${note})}"
    add release build_path - PASS "${note:-all-checked}"
  else
    bad "release build path FAILED — $fails"
    add release build_path - FAIL "$fails"
  fi
}
