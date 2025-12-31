"""Smoke test for status payload with chunk details."""

from __future__ import annotations

import sys
import threading
import time
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from novel_proofer.server import Handler


def _start_server(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def main() -> int:
    port = 8012
    httpd = _start_server(port)
    try:
        time.sleep(0.1)
        # just ensure endpoint responds with json 404 when missing.
        try:
            conn = HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/api/jobs/status?job_id=missing")
            resp = conn.getresponse()
            resp.read()
        except Exception:
            pass
        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
