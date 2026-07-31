"""Cloud export runners — MinIO/S3, REAL GCS, fake-GCS.

Port of the three one-shot wrappers in `dev/cloud/`, each of which ensures its
bucket exists and then hands off to `cargo run --bin rivet -- <sub> --config …`:

* `dev/cloud/run_s3_export.sh`        → `s3()`        (MinIO on :9000, bucket `rivet-test`)
* `dev/cloud/run_gcs_export.sh`       → `gcs()`       (**real** GCS bucket `rivet_data_test`)
* `dev/cloud/run_gcs_fake_export.sh`  → `gcs_fake()`  (fake-gcs-server on :4443)

Usage (the first positional is the rivet SUBCOMMAND, exactly as `$1` was):

    python3 dev/pytools/cloud_exports.py s3                    # rivet run
    python3 dev/pytools/cloud_exports.py s3 check              # preflight
    python3 dev/pytools/cloud_exports.py gcs run --validate
    python3 dev/pytools/cloud_exports.py gcs-fake check

Env, unchanged: `RIVET_CONFIG`, `RUST_LOG` (default `info`), `MINIO_ACCESS_KEY`
/ `MINIO_SECRET_KEY` (default `minioadmin`), `DATABASE_URL` (gcs), `RIVET_RELEASE=1`
(gcs, `cargo run --release`), `FAKE_GCS_BUCKET` / `FAKE_GCS_URL` (gcs-fake).

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH (each one a bug it shipped):

1. **A missing credential is a loud SKIP (exit 2), never a run.** None of the
   three scripts checked anything: `run_gcs_export.sh` in particular exec'd
   straight into a rivet run against the REAL bucket `rivet_data_test`, so an
   absent service-account file surfaced minutes later as an opaque OpenDAL 401
   — after the source had already been read. Every `*_key_env` / `*_token_env`
   the config names must be set, and a real-GCS destination must have a
   `credentials_file` that exists (or `GOOGLE_APPLICATION_CREDENTIALS`).
   `allow_anonymous: true` (the emulator) needs neither and is not asked for it.
2. **A dead store is a loud SKIP too, not a "may already exist" shrug.**
   `run_gcs_fake_export.sh` ran `curl -sf … >/dev/null` and printed
   "(bucket may already exist or server not ready)" for BOTH the 409-exists case
   and "nothing is listening on 4443" — bug class 8, a suppressed client error
   resurfacing later as a nonsense verdict. HTTP 409 still prints the bash line
   and continues; a transport error prints it and then stops with exit 2.
3. `run_s3_export.sh` computed `STATUS=$(curl -s -o /dev/null -w %{http_code}
   -X PUT …)` and **never read `STATUS`** — a dead unauthenticated PUT duplicating
   the `python3 -c` block two lines below it (which does the same PUT via
   urllib). Dropped: it produced no output, so nothing observable changes.
   MinIO answers that unsigned PUT with 403, which is why the surviving
   bucket-ensure usually prints `bucket creation: HTTP 403 (may need manual
   creation)` — the bucket has to pre-exist, and the printed hint says so.
4. Nothing is deleted, ever. These scripts create a bucket at most; a cloud
   prefix they did not create is never touched (and neither is one they did).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/cloud_exports.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT


def _say(msg: str = "") -> None:
    """stdout, FLUSHED. These lines interleave with CHILD output on the same fd,
    and a block-buffered `print` lands after it whenever stdout is a pipe or a
    file — the bash `echo` was unbuffered, so its ordering was always right.
    (Seen for real: "Ensuring bucket …" printed *after* the whole cargo run.)"""
    print(msg, flush=True)


USAGE = (
    "usage: dev/pytools/cloud_exports.py s3       [rivet-subcommand] [args…]\n"
    "       dev/pytools/cloud_exports.py gcs      [rivet-subcommand] [args…]\n"
    "       dev/pytools/cloud_exports.py gcs-fake [rivet-subcommand] [args…]"
)

S3_CONFIG = "dev/cloud/rivet_s3_minio_test.yaml"
GCS_CONFIG = "dev/cloud/rivet_gcs_rivet_data_test.yaml"
GCS_FAKE_CONFIG = "dev/cloud/rivet_gcs_fake_test.yaml"

MINIO_BASE = "http://localhost:9000"
MINIO_CONSOLE = "http://localhost:9001"
S3_BUCKET = "rivet-test"


# ── config ─────────────────────────────────────────────────────────────────────
def _config_path(explicit: str | Path | None, default: str) -> Path:
    """`RIVET_CONFIG` or the per-script default, resolved against the repo root.

    The bash `cd`'d to the root and passed a relative path; a caller in another
    directory got a config-not-found from rivet with no hint about the cwd.
    """
    spelled = str(explicit or os.environ.get("RIVET_CONFIG") or default)
    path = Path(spelled)
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise shell.Fail(
            f"config not found: {spelled}",
            code=2,
            hint="set RIVET_CONFIG=<path> or run from a checkout that has "
                 f"{default}",
        )
    return path


_KEY_ENV = re.compile(
    r"^\s*(access_key_env|secret_key_env|session_token_env|account_key_env|sas_token_env)"
    r"\s*:\s*(\S+)\s*$",
    re.M,
)
_CRED_FILE = re.compile(r"^\s*credentials_file\s*:\s*(.+?)\s*$", re.M)
_ANON = re.compile(r"^\s*allow_anonymous\s*:\s*true\s*$", re.M)
_GCS_DEST = re.compile(r"^\s*type\s*:\s*[\"']?gcs[\"']?\s*$", re.M)
_ENDPOINT = re.compile(r"^\s*endpoint\s*:\s*[\"']?(\S+?)[\"']?\s*$", re.M)


def _unquote(value: str) -> str:
    return value.strip().strip("\"'")


def _require_credentials(cfg: Path, text: str, env: dict[str, str]) -> None:
    """Every credential the config NAMES must be resolvable, or this is a SKIP.

    A missing credential is the single worst thing to be quiet about here: these
    configs point at real object stores, so "it ran and reported success" with no
    credential means either nothing was written or something was written
    somewhere unintended. Exit 2 marks it as an ENVIRONMENT gap (the same code
    the source-parity sweeps use), never a pass.
    """
    missing = [
        name
        for _, raw in _KEY_ENV.findall(text)
        for name in [_unquote(raw)]
        if not (env.get(name) or os.environ.get(name))
    ]
    if missing:
        shell.skip(
            f"{cfg.name}: credential env var(s) not set: {', '.join(missing)} "
            "— SKIPPING (refusing to run a cloud export without credentials)"
        )
        raise shell.Fail(
            f"absent cloud credentials for {cfg.name}",
            code=2,
            hint=f"export {missing[0]}=… (see docs/cloud-auth.md)",
        )

    if not _GCS_DEST.search(text) or _ANON.search(text):
        # No GCS destination, or an emulator destination that takes unsigned
        # requests: there is no service account to look for.
        return
    declared = [Path(os.path.expanduser(_unquote(p))) for p in _CRED_FILE.findall(text)]
    adc = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if any(p.is_file() for p in declared):
        return
    if adc and Path(os.path.expanduser(adc)).is_file():
        return
    named = ", ".join(str(p) for p in declared) or "(none declared)"
    shell.skip(
        f"{cfg.name}: no usable GCS service-account credentials — SKIPPING. "
        f"credentials_file: {named}; GOOGLE_APPLICATION_CREDENTIALS: {adc or 'unset'}"
    )
    raise shell.Fail(
        f"absent GCS credentials for {cfg.name} (this export writes to a REAL bucket)",
        code=2,
        hint="point credentials_file: at a service-account JSON, or export "
             "GOOGLE_APPLICATION_CREDENTIALS=/path/sa.json",
    )


# ── bucket ensure ──────────────────────────────────────────────────────────────
def _http(
    url: str, *, method: str, body: bytes | None = None, headers: dict[str, str] | None = None
) -> int:
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return int(getattr(resp, "status", 0) or 200)


def _ensure_minio_bucket(base: str, bucket: str) -> None:
    """MinIO accepts a plain `PUT /<bucket>` from an authorised client.

    The messages are the bash's, verbatim — including the fact that an
    unauthenticated PUT normally lands as 403, so the operator is told to create
    the bucket in the console. Only the transport-error arm is new: nothing
    listening means the whole export is pointless, so say so and stop.
    """
    url = f"{base.rstrip('/')}/{bucket}"
    try:
        _http(url, method="PUT")
        _say("  bucket created")
    except urllib.error.HTTPError as e:
        if e.code == 409:
            _say("  bucket already exists")
        else:
            _say(f"  bucket creation: HTTP {e.code} (may need manual creation)")
    except (urllib.error.URLError, OSError) as e:
        _say(f"  could not auto-create bucket: {e}")
        _say(f"  create it manually at {MINIO_CONSOLE} (minioadmin/minioadmin)")
        raise shell.Fail(
            f"MinIO is not answering at {base} — SKIPPING the export",
            code=2,
            hint="docker compose up -d minio",
        )


def _ensure_gcs_bucket(base: str, bucket: str) -> None:
    """fake-gcs-server's JSON API: `POST /storage/v1/b?project=test`.

    409 = already exists, which the bash could not tell from "server not ready"
    because both arrived as a non-zero `curl -sf`. The 409 line is kept as-is;
    an unreachable emulator now stops the run instead of handing rivet a dead
    endpoint.
    """
    url = f"{base.rstrip('/')}/storage/v1/b?project=test"
    body = json.dumps({"name": bucket}).encode()
    try:
        _http(url, method="POST", body=body, headers={"Content-Type": "application/json"})
        _say(f"  created bucket {bucket}")
    except urllib.error.HTTPError:
        # 409 (exists) and any other HTTP answer: the server IS up, so the bash's
        # optimism is justified — rivet is allowed to try.
        _say("  (bucket may already exist or server not ready)")
    except (urllib.error.URLError, OSError) as e:
        _say("  (bucket may already exist or server not ready)")
        raise shell.Fail(
            f"fake-gcs-server is not answering at {base} ({e}) — SKIPPING the export",
            code=2,
            hint="docker compose up -d fake-gcs",
        )


# ── the handoff ────────────────────────────────────────────────────────────────
def _cargo_rivet(
    sub: str,
    config: Path,
    extra: Sequence[str] = (),
    *,
    release: bool = False,
    env: dict[str, str] | None = None,
) -> int:
    """`exec cargo run [--release] --bin rivet -- <sub> --config <cfg> "$@"`.

    Streamed, not captured: watching the export IS the point of these runners,
    and the exit code is passed straight through (the bash `exec`'d, so rivet's
    status WAS the script's status).
    """
    shell.require("cargo", hint="install Rust: https://rustup.rs")
    argv = ["cargo", "run"]
    if release:
        argv.append("--release")
    argv += ["--bin", "rivet", "--", sub, "--config", str(config), *extra]
    return shell.stream(argv, cwd=ROOT, env=env, timeout=None).returncode


def s3(sub: str = "run", extra: Sequence[str] = (), *, config: str | Path | None = None) -> int:
    """S3 export against local MinIO (`dev/cloud/run_s3_export.sh`).

    Prerequisites, unchanged: `docker compose up -d minio postgres` and a seeded
    database (`cargo run --bin seed -- --target postgres --users 1000`).
    """
    cfg = _config_path(config, S3_CONFIG)
    env = {
        "RUST_LOG": os.environ.get("RUST_LOG") or "info",
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY") or "minioadmin",
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY") or "minioadmin",
    }
    text = cfg.read_text()
    _require_credentials(cfg, text, env)
    _say(f"Ensuring bucket '{S3_BUCKET}' exists in MinIO...")
    _ensure_minio_bucket(MINIO_BASE, S3_BUCKET)
    return _cargo_rivet(sub, cfg, extra, env=env)


def gcs(sub: str = "run", extra: Sequence[str] = (), *, config: str | Path | None = None) -> int:
    """Chunked exports to the REAL GCS bucket (`dev/cloud/run_gcs_export.sh`).

    `url_env: DATABASE_URL` in the config is why the URL is exported rather than
    inlined: `rivet plan` redacts credentials in the artifact, so plan+apply need
    the env var on both legs.
    """
    cfg = _config_path(config, GCS_CONFIG)
    env = {
        "DATABASE_URL": os.environ.get("DATABASE_URL")
        or "postgresql://rivet:rivet@localhost:5432/rivet",
        "RUST_LOG": os.environ.get("RUST_LOG") or "info",
    }
    _require_credentials(cfg, cfg.read_text(), env)
    return _cargo_rivet(
        sub, cfg, extra, release=os.environ.get("RIVET_RELEASE") == "1", env=env
    )


def gcs_fake(
    sub: str = "run", extra: Sequence[str] = (), *, config: str | Path | None = None
) -> int:
    """Export to the local fake-gcs-server (`dev/cloud/run_gcs_fake_export.sh`).

    Prerequisites, unchanged: `docker compose up -d fake-gcs postgres` and a
    seeded `orders` table.
    """
    bucket = os.environ.get("FAKE_GCS_BUCKET") or "rivet-fake"
    base = os.environ.get("FAKE_GCS_URL") or "http://127.0.0.1:4443"
    env = {"RUST_LOG": os.environ.get("RUST_LOG") or "info"}
    cfg = _config_path(config, GCS_FAKE_CONFIG)
    text = cfg.read_text()
    _require_credentials(cfg, text, env)

    endpoint = _ENDPOINT.search(text)
    if endpoint and urlparse(_unquote(endpoint.group(1))).port not in (
        urlparse(base).port,
        None,
    ):
        # The bucket is created on FAKE_GCS_URL but rivet writes to the config's
        # endpoint; if they disagree the ensure step silently primed the wrong
        # store. Cheap to notice, invisible in the bash.
        shell.warn(
            f"{cfg.name} writes to {_unquote(endpoint.group(1))} but the bucket is "
            f"being ensured on {base} (set FAKE_GCS_URL to match)"
        )

    _say(f"Ensuring bucket '{bucket}' exists on fake-gcs-server...")
    _ensure_gcs_bucket(base, bucket)
    return _cargo_rivet(sub, cfg, extra, env=env)


# ══ CLI ════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    target = args[0] if args else ""
    # Positional exactly as the bash: `$1` is rivet's subcommand, `"$@"` the rest.
    sub = args[1] if len(args) > 1 else "run"
    extra = args[2:]

    if target == "s3":
        return s3(sub, extra)
    if target == "gcs":
        return gcs(sub, extra)
    if target in ("gcs-fake", "gcs_fake", "fake-gcs"):
        return gcs_fake(sub, extra)

    _say(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
