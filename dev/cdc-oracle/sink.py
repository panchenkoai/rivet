#!/usr/bin/env python3
"""Minimal HTTP sink for Debezium Server — appends every change event to a JSONL file.

Debezium Server's `http` sink POSTs a JSON array of events. We keep the raw
payload: normalisation belongs in the comparison step, not here, so the captured
file stays a faithful record of what the reference tool emitted.
"""
import json, os, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUT = os.environ.get("DBZ_SINK_OUT", "/data/debezium.jsonl")

class H(BaseHTTPRequestHandler):
    # HTTP/1.1 with keep-alive, and it is load-bearing. The default HTTP/1.0
    # closes the connection after each response, while Debezium's Java client
    # reuses it — the next POST then hits a closed socket and the connector logs
    # `IOException: HTTP/1.1 header parser received no bytes` and DROPS that
    # batch. Measured on a 75-change scenario: 30, 39, 57 events delivered across
    # three identical runs, which read as the reference falling short when it was
    # the sink losing them.
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = [{"_unparsed": body.decode("utf-8", "replace")}]
        events = payload if isinstance(payload, list) else [payload]
        with _LOCK, open(OUT, "a") as f:
            for e in events:
                f.write(json.dumps(e, sort_keys=True) + "\n")
        # An explicit zero-length body: under HTTP/1.1 a response without either
        # Content-Length or a chunked encoding leaves the client waiting, which is
        # the same lost batch by a slower route.
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def log_message(self, *a):
        pass

class Server(ThreadingHTTPServer):
    # A single-threaded HTTPServer with the default 5-deep listen queue DROPPED
    # connections under Debezium's batch delivery, and the loss looked like the
    # reference falling short: the same 75-change scenario delivered 30, then 39,
    # then 57 across three runs. Threaded, with a deep queue, so the harness stops
    # manufacturing the disagreements it is supposed to measure.
    daemon_threads = True
    request_queue_size = 128


# One lock around the append: threads now serve concurrently, and interleaved
# writes would corrupt the very capture this file exists to preserve.
_LOCK = threading.Lock()

if __name__ == "__main__":
    port = int(os.environ.get("DBZ_SINK_PORT", "8088"))
    print(f"debezium sink on :{port} -> {OUT}", flush=True)
    Server(("0.0.0.0", port), H).serve_forever()
