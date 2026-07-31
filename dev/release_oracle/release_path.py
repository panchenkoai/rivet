"""The RELEASE BUILD/PUBLISH path stage of the release oracle.

Port of `dev/release-oracle/lib/release_path.sh`.

The batch/CDC scenarios prove the CORRECTNESS of what ships; they assume a
working binary already exists. But the release pipeline runs DIFFERENT, stricter
tooling than `cargo build`, and the gap only surfaces at the TAG — after
crates.io, the binaries and the GitHub release have published, when the failure
is no longer re-runnable from the immutable tag:

* 0.16.0 shipped broken Docker images. The Dockerfile's `cargo chef prepare`
  parses `Cargo.toml` with `cargo-manifest` (spec-strict), which rejects the
  multi-line inline tables `cargo`'s own parser tolerates. Green everywhere else.
* 0.16.1: `cargo publish --locked` aborted on a `Cargo.lock` not committed in
  sync with the bumped `Cargo.toml` — again AFTER the tag was cut.

CI guards both now, but the go/no-go GATE (this oracle) did not — so a pre-tag
run could be green while the release build path was broken. This stage runs the
real path (or its exact sub-steps), so the mismatch fails LOUD and pre-tag.

ORDERING IS LOAD-BEARING, at two levels:

1. This function must be the FIRST thing the driver calls, before any other
   cargo command touches the working tree.
2. Inside it, the `cargo metadata --locked` lock-sync check must come before the
   `cargo test` / `cargo chef` steps.

Both for the same reason: a stale committed lock only exists in git, and any
`cargo build`/`cargo test` silently RECONCILES the working-tree lock before a
later check could observe the staleness. Check the committed state first, or
check nothing.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from .core import ROOT, Ledger, Proc, docker, have, run

# ── scratch dir ────────────────────────────────────────────────────────────────
# The bash driver owned a single `$WORK=$(mktemp -d)` for the whole run and this
# stage wrote `recipe.json` / `docker_build.log` into it. Honour an explicit
# work dir when the driver exports one, else make our own on first use.
#
# Deliberately NOT auto-deleted: the failure message points the reader at
# `docker_build.log`, and bash's own `trap cleanup EXIT` removed `$WORK` before
# anyone could open it. Leaving the temp dir behind makes the hint truthful.
_work: Path | None = None


def _work_dir() -> Path:
    global _work
    if _work is None:
        env = os.environ.get("RIVET_ORACLE_WORK")
        _work = Path(env) if env else Path(tempfile.mkdtemp(prefix="rivet-oracle-"))
        _work.mkdir(parents=True, exist_ok=True)
    return _work


# ── the stage ──────────────────────────────────────────────────────────────────
# Identity columns for every row this stage records, matching the bash `add`
# calls (`add release build_path - <STATUS> "<detail>"`).
_ENGINE = "release"
_VERSION = "build_path"
_SCENARIO = "-"
_STORE = "-"


def _cargo(*args: str, timeout: float | None = None) -> Proc:
    """A cargo invocation from the repo root — the bash used `( cd "$ROOT" && … )`,
    a subshell purely to scope the `cd`; `cwd=` is the same thing without one."""
    return run(["cargo", *args], cwd=ROOT, timeout=timeout)


def verify_release_build_path(led: Ledger) -> None:
    if not have("cargo"):
        led.skipped(_ENGINE, _VERSION, _SCENARIO, _STORE,
                    "release build path: cargo absent", "no cargo")
        return

    led.phase(
        "Release build path (lock sync / manifest-chef parse / schema regen / "
        "cargo-chef planner / [docker image])"
    )
    # Both accumulate space-suffixed fragments, joined verbatim, so the rendered
    # message and the recorded detail match the bash byte for byte.
    fails: list[str] = []
    note: list[str] = []

    # 1. Cargo.lock in sync with Cargo.toml — the release publishes with
    #    `cargo publish --locked` and the Docker builder cooks + builds `--locked`
    #    (0.16.1). FIRST: `cargo metadata --locked` resolves without compiling
    #    (~seconds) AND before anything reconciles the lock out from under it.
    if not _cargo("metadata", "--locked", "--format-version", "1").ok:
        fails.append("lock-out-of-sync(publish/docker --locked would abort) ")

    # 2. The offline guards that mirror the release-only tooling: the
    #    cargo-manifest strict parse (0.16.0 — no multi-line inline tables in
    #    Cargo.toml) and the JSON-schema regen (a version bump that forgot
    #    `rivet schema config`). Short-circuited like the bash `&&` chain: if the
    #    manifest guard fails we never run the schema one, and either failure is
    #    the one fragment below.
    manifest_guard = _cargo("test", "--test", "offline_suite", "cargo_manifest_chef")
    if not (manifest_guard.ok
            and _cargo("test", "--test", "offline_suite", "schema_drift").ok):
        fails.append("offline-release-guard(manifest-chef/schema-drift) ")

    # 3. The ACTUAL first Docker step — `cargo chef prepare` (Dockerfile stage
    #    `planner`). It distils the dep graph with NO compilation, so it is cheap
    #    yet runs the exact cargo-manifest parse the 0.16.0 image failed on: a
    #    proxy that IS the real command, not a stand-in. Noted-skip when
    #    cargo-chef is not installed (an absent tool is never a pass).
    if _cargo("chef", "--version").ok:
        recipe = _work_dir() / "recipe.json"
        if not _cargo("chef", "prepare", "--recipe-path", str(recipe)).ok:
            fails.append("cargo-chef-prepare(the Docker planner step) ")
    else:
        note.append("cargo-chef absent (docker planner step not run) ")

    # 4. The full release Docker image — the complete real path (0.16.0 was ONLY
    #    caught here). Minutes (fat-LTO in-image), so opt-in via
    #    RIVET_ORACLE_DOCKER=1; the steps above cover the known regression
    #    classes cheaply on every run.
    if os.environ.get("RIVET_ORACLE_DOCKER", "0") == "1":
        if have("docker"):
            log = _work_dir() / "docker_build.log"
            # No timeout, as in bash: an in-image fat-LTO build legitimately runs
            # for many minutes and a killed build would read as a real failure.
            build = docker("build", "-f", "Dockerfile", "-t",
                           "rivet-oracle-release-check", ".", cwd=ROOT, timeout=None)
            log.write_text(build.out)
            if not build.ok:
                fails.append(f"docker-image-build(see {log}) ")
        else:
            note.append("docker absent (image build skipped) ")
    else:
        note.append("full docker build skipped (set RIVET_ORACLE_DOCKER=1) ")

    notes = "".join(note)
    if not fails:
        suffix = f" ({notes})" if notes else ""
        led.passed(
            _ENGINE, _VERSION, _SCENARIO, _STORE,
            "release build path: lock in sync, manifest-chef + schema guards pass, "
            f"cargo-chef planner ok{suffix}",
            notes or "all-checked",
        )
    else:
        joined = "".join(fails)
        led.failed(_ENGINE, _VERSION, _SCENARIO, _STORE,
                   f"release build path FAILED — {joined}", joined)
