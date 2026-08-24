#!/usr/bin/env python3
"""Minimal HTTP sink for Debezium Server — appends every change event to a JSONL file.

Debezium Server's `http` sink POSTs a JSON array of events. We keep the raw
payload: normalisation belongs in the comparison step, not here, so the captured
file stays a faithful record of what the reference tool emitted.
"""
import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

OUT = os.environ.get("DBZ_SINK_OUT", "/data/debezium.jsonl")

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = [{"_unparsed": body.decode("utf-8", "replace")}]
        events = payload if isinstance(payload, list) else [payload]
        with open(OUT, "a") as f:
            for e in events:
                f.write(json.dumps(e, sort_keys=True) + "\n")
        self.send_response(204)
        self.end_headers()
    def log_message(self, *a):
        pass

if __name__ == "__main__":
    port = int(os.environ.get("DBZ_SINK_PORT", "8088"))
    print(f"debezium sink on :{port} -> {OUT}", flush=True)
    HTTPServer(("0.0.0.0", port), H).serve_forever()
