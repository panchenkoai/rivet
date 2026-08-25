#!/usr/bin/env python3
"""HTTP sink for Debezium Server — records every change event as JSONL.

Written to be a FAITHFUL record of what the reference emitted, because everything
downstream compares against it. Three things the first version got wrong, each of
which silently corrupted the comparison rather than failing:

1. **HTTP/1.0.** The default closes the connection after each response while
   Debezium's Java client keeps it alive; the next POST hit a closed socket, the
   connector logged `IOException: HTTP/1.1 header parser received no bytes` and
   DROPPED the batch. Measured: 30, 39, 57 of 75 events across identical runs,
   which read as the reference falling short.
2. **Single-threaded, listen queue 5.** Connections refused under batch delivery.
3. **Headers discarded.** Debezium carries metadata in `X-DEBEZIUM-*` headers
   (base64-encoded Connect values) — the message key among them. Throwing them
   away is why MongoDB deletes could not be compared by key.

So: HTTP/1.1 with an explicit Content-Length, threaded with a deep queue, and the
headers merged into each record under `__headers` rather than dropped.
"""
import base64, json, os, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUT = os.environ.get("DBZ_SINK_OUT", "/data/debezium.jsonl")
_LOCK = threading.Lock()


def _decode(v: str):
    """Debezium base64-encodes header values as serialised Connect data. Decode
    when it round-trips as JSON; otherwise keep the raw string — a lossy guess
    would be worse than an unparsed value the comparison can still see."""
    try:
        raw = base64.b64decode(v, validate=True).decode("utf-8")
    except Exception:
        return v
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


class H(BaseHTTPRequestHandler):
    # Load-bearing: see the module docstring. HTTP/1.0 loses batches.
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = [{"_unparsed": body.decode("utf-8", "replace")}]
        events = payload if isinstance(payload, list) else [payload]
        hdrs = {k: _decode(v) for k, v in self.headers.items()
                if k.upper().startswith("X-DEBEZIUM-")}
        with _LOCK, open(OUT, "a") as f:
            for e in events:
                if isinstance(e, dict) and hdrs:
                    e = {**e, "__headers": hdrs}
                f.write(json.dumps(e, sort_keys=True) + "\n")
        # An explicit zero-length body: under HTTP/1.1 a response with neither a
        # Content-Length nor chunked encoding leaves the client waiting, which
        # loses the batch by a slower route.
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *a):
        pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


if __name__ == "__main__":
    port = int(os.environ.get("DBZ_SINK_PORT", "8088"))
    print(f"debezium sink on :{port} -> {OUT}", flush=True)
    Server(("0.0.0.0", port), H).serve_forever()
