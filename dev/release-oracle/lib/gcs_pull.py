#!/usr/bin/env python3
"""Download every .parquet under a prefix from the fake-gcs emulator into a local
dir — an INDEPENDENT readback for the load gate when `gsutil` is not installed.

Uses only the fake-gcs JSON API (list) + its media download endpoint, so it never
routes through rivet: a rivet write bug cannot rubber-stamp its own output.

Usage: gcs_pull.py <endpoint> <bucket> <prefix> <dest_dir>
Prints the number of parquet objects downloaded.
"""
import os
import sys
import urllib.parse
import urllib.request

endpoint, bucket, prefix, dest = sys.argv[1:5]
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
    if not name.endswith(".parquet"):
        continue
    media = f"{endpoint}/download/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}?alt=media"
    try:
        urllib.request.urlretrieve(media, os.path.join(dest, f"part_{n}.parquet"))
        n += 1
    except Exception:
        pass
print(n)
