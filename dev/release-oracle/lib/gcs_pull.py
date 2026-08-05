#!/usr/bin/env python3
"""Download every .parquet under a prefix from the fake-gcs emulator into a local
dir — an INDEPENDENT readback for the load gate when `gsutil` is not installed.

Uses only the fake-gcs JSON API (list) + its media download endpoint, so it never
routes through rivet: a rivet write bug cannot rubber-stamp its own output.

Usage: gcs_pull.py <endpoint> <bucket> <prefix> <dest_dir> [--all]
Prints the number of objects downloaded.

`--all` pulls EVERY object and preserves its path under the prefix, rather than
only `.parquet` flattened to `part_N.parquet`. A caller that needs to know what
the run DECLARED — the manifests — cannot answer that from the parquet alone,
and the flattened names break any manifest that names its parts. Opt-in so the
default shape and printed count stay exactly what the load gate already reads.
"""
import os
import sys
import urllib.parse
import urllib.request

endpoint, bucket, prefix, dest = sys.argv[1:5]
pull_all = "--all" in sys.argv[5:]
os.makedirs(dest, exist_ok=True)

list_url = f"{endpoint}/storage/v1/b/{bucket}/o?prefix={urllib.parse.quote(prefix, safe='')}"
try:
    with urllib.request.urlopen(list_url, timeout=10) as r:
        import json
        items = json.load(r).get("items", [])
except Exception:
    print(0)
    sys.exit(0)

n = 0
for it in items:
    name = it.get("name", "")
    if not pull_all and not name.endswith(".parquet"):
        continue
    media = f"{endpoint}/download/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}?alt=media"
    if pull_all:
        rel = name[len(prefix):].lstrip("/") or os.path.basename(name)
        out = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(out) or dest, exist_ok=True)
    else:
        out = os.path.join(dest, f"part_{n}.parquet")
    try:
        urllib.request.urlretrieve(media, out)
        n += 1
    except Exception:
        pass
print(n)
