"""Real GCP (BigQuery + GCS) over REST with one cached token and a kept-alive connection per thread.

Replaces per-call `bq` / `gcloud storage` CLI starts in the gate: measured 2026-09-23,
a CLI call costs 1.7 s (`bq`), 3.3 s (`bq query`) or 4.6 s (`gcloud storage rm`); the
same request over a reused connection is 0.25–0.8 s.
"""

from __future__ import annotations

import http.client
import json
import subprocess
import threading
import time
import urllib.parse

_TOKEN_TTL = 40 * 60  # access tokens live 60 min
_token_lock = threading.Lock()
_token: tuple[str, float] = ("", 0.0)
_local = threading.local()


def token() -> str:
    """The `gcloud auth print-access-token` identity, fetched once per TTL."""
    global _token
    with _token_lock:
        tok, at = _token
        if not tok or time.monotonic() - at > _TOKEN_TTL:
            tok = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True,
                                 text=True, check=True).stdout.strip()
            _token = (tok, time.monotonic())
        return tok


def _call(host: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """(status, parsed JSON body) for one request on this thread's connection to `host`."""
    conns = _local.__dict__.setdefault("conns", {})
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token()}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    for attempt in (0, 1):  # a server-closed keep-alive fails once; reconnect and retry
        conn = conns.get(host) or http.client.HTTPSConnection(host, timeout=120)
        conns[host] = conn
        try:
            conn.request(method, path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
        except (http.client.HTTPException, OSError):
            conn.close()
            conns.pop(host, None)
            if attempt:
                raise
    raise AssertionError("unreachable")


def _bq(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    return _call("bigquery.googleapis.com", method, f"/bigquery/v2{path}", body)


def bq_ensure_dataset(project: str, dataset: str, location: str = "US") -> None:
    """Create the dataset unless it exists (`bq mk -f`)."""
    st, b = _bq("POST", f"/projects/{project}/datasets",
                {"datasetReference": {"projectId": project, "datasetId": dataset},
                 "location": location})
    if st not in (200, 409):
        raise RuntimeError(f"bigquery: create dataset {dataset} → {st} {b}")


def bq_delete_table(project: str, dataset: str, table: str) -> None:
    """Drop the table if present (`bq rm -f -t`)."""
    st, b = _bq("DELETE", f"/projects/{project}/datasets/{dataset}/tables/{table}")
    if st not in (200, 204, 404):
        raise RuntimeError(f"bigquery: delete table {dataset}.{table} → {st} {b}")


def bq_delete_dataset(project: str, dataset: str) -> None:
    """Drop the dataset and its contents if present (`bq rm -r -f -d`)."""
    st, b = _bq("DELETE", f"/projects/{project}/datasets/{dataset}?deleteContents=true")
    if st not in (200, 204, 404):
        raise RuntimeError(f"bigquery: delete dataset {dataset} → {st} {b}")


def bq_dataset_exists(project: str, dataset: str) -> bool:
    """Does the dataset exist (`bq show -d` succeeding)?"""
    st, b = _bq("GET", f"/projects/{project}/datasets/{dataset}")
    if st not in (200, 404):
        raise RuntimeError(f"bigquery: get dataset {dataset} → {st} {b}")
    return st == 200


def bq_scalar(project: str, sql: str) -> str | None:
    """First cell of a standard-SQL query, or None when the query did not answer."""
    st, b = _bq("POST", f"/projects/{project}/queries",
                {"query": sql, "useLegacySql": False, "timeoutMs": 120_000})
    if st != 200 or not b.get("jobComplete") or not b.get("rows"):
        return None
    return b["rows"][0]["f"][0]["v"]


def gcs_delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every object under `prefix` (`gcloud storage rm -r`); the count deleted."""
    host, n, page = "storage.googleapis.com", 0, ""
    while True:
        q = urllib.parse.urlencode({"prefix": prefix, "fields": "items(name),nextPageToken",
                                    **({"pageToken": page} if page else {})})
        st, b = _call(host, "GET", f"/storage/v1/b/{bucket}/o?{q}")
        if st != 200:
            raise RuntimeError(f"gcs: list gs://{bucket}/{prefix} → {st} {b}")
        for item in b.get("items", []):
            name = urllib.parse.quote(item["name"], safe="")
            dst, db = _call(host, "DELETE", f"/storage/v1/b/{bucket}/o/{name}")
            if dst not in (200, 204, 404):
                raise RuntimeError(f"gcs: delete {item['name']} → {dst} {db}")
            n += 1
        page = b.get("nextPageToken", "")
        if not page:
            return n
