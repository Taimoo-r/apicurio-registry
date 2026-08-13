#!/usr/bin/env python3
"""
Mock webhook receiver for the Apicurio Registry CloudEvents delivery demo.

Logs every delivery with a timestamp, verifies the HMAC signature, and can be told to reject the first N
attempts so the retry and restart-resume behaviour is observable.

  python receiver.py --port 9000 --secret s3cr3t --fail-first 3
"""
import argparse
import hashlib
import hmac
import json
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

ARGS = None
ATTEMPTS = {}


def stamp():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        pass  # silence the default per-request stderr logging

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        delivery_id = self.headers.get("X-Registry-Delivery-Id", "?")
        event_type = self.headers.get("X-Registry-Event-Type", "?")
        attempt = self.headers.get("X-Registry-Delivery-Attempt", "?")
        signature = self.headers.get("X-Registry-Signature")

        sig_state = "unsigned"
        if ARGS.secret:
            expected = "sha256=" + hmac.new(
                ARGS.secret.encode("utf-8"), raw, hashlib.sha256
            ).hexdigest()
            sig_state = "valid" if signature and hmac.compare_digest(expected, signature) else "INVALID"

        ATTEMPTS[delivery_id] = ATTEMPTS.get(delivery_id, 0) + 1
        seen = ATTEMPTS[delivery_id]
        reject = seen <= ARGS.fail_first

        status = 500 if reject else 204
        verdict = "REJECT 500" if reject else "ACCEPT 204"

        try:
            body = json.loads(raw.decode("utf-8"))
            ce_id = body.get("id", "?")
            ce_type = body.get("type", event_type)
            data = body.get("data", {})
            subject = "{}/{}".format(data.get("artifactId", "?"), data.get("version", "?"))
        except Exception:
            ce_id, ce_type, subject = "?", event_type, "?"

        print(
            "{}  {:<11}  attempt={:<3} sig={:<8} {:<48} {} (delivery {})".format(
                stamp(), verdict, attempt, sig_state, ce_type, subject, delivery_id[:8]
            ),
            flush=True,
        )

        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main():
    global ARGS
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--secret", default=None)
    p.add_argument("--fail-first", type=int, default=0,
                   help="reject the first N attempts of each delivery with HTTP 500")
    ARGS = p.parse_args()

    print("receiver listening on :{}  fail-first={}  secret={}".format(
        ARGS.port, ARGS.fail_first, "set" if ARGS.secret else "none"), flush=True)
    try:
        HTTPServer(("0.0.0.0", ARGS.port), Handler).serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
